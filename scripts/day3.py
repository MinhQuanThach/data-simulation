"""Host-only day-3 operator commands and real Airflow/standalone acceptance.

No HTML or count oracle is fabricated for web runs. Local HTTP tests are separate.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import re
import subprocess
import time
import unicodedata
from uuid import NAMESPACE_URL, UUID, uuid5

from day1 import ROOT, command, compose, query, redact, save, wait_until
from day2 import scheduler, task_states

DAG = "bronze_to_silver"
EVIDENCE = ROOT / "docs/evidence/day3"
SOURCE_FILES = ["compose.yaml", "requirements/airflow.in", "dags/bronze_to_silver.py",
                "scripts/init-lab.py", "scripts/day2.py", "scripts/day3.py", "tests/test_day2.py",
                "tests/test_day3.py", "config/day3_acceptance.json"] + [
    path.relative_to(ROOT).as_posix() for folder, pattern in (("src", "*.py"), ("sql", "*.sql"))
    for path in sorted((ROOT / folder).rglob(pattern))]


def source_hashes():
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCE_FILES}


def snapshot(run_id, top_n=3):
    return json.loads(scheduler("python", "-m", "src.crawler.pipeline", "inspect", "--run-id",
                                str(UUID(run_id)), "--top-n", str(top_n)))


def collect(run, phase="initial"):
    dest = run["evidence"] / phase
    air_id = run["airflow_run_id"]
    if not re.fullmatch(r"day3_[A-Za-z0-9_]+", air_id):
        raise ValueError("Unexpected acceptance run ID")
    logs = scheduler("python", "-c", "from pathlib import Path; p=Path('/opt/airflow/logs/dag_id=" + DAG +
                     "/run_id=" + air_id + "'); [print(str(f)+'\\n'+f.read_text()) for f in sorted(p.rglob('*.log'))]")
    save(dest / "task-logs.txt", logs)
    report = {"checked_at_utc": datetime.now(timezone.utc).isoformat(),
              "airflow_run_id": air_id, "run_id": run["run_id"], "logical_date": run["logical_date"],
              "dag_state": run["state"], "conf": run["conf"], "tasks": task_states(air_id),
              "source_sha256": source_hashes(), "scope": "day3 real HTTP web crawl"}
    save(dest / "report.json", json.dumps(report, indent=2))
    output = snapshot(run["run_id"], run["conf"].get("top_n", 3))
    save(dest / "sql-output.json", json.dumps(output, ensure_ascii=False, indent=2))
    for page in output["bronze"]:
        if page["raw_body_hex"] is not None:
            # Exact source bytes, not the decoded/redacted text output.
            dest.mkdir(parents=True, exist_ok=True)
            (dest / (page["page_id"] + ".body")).write_bytes(bytes.fromhex(page["raw_body_hex"]))
        if page["raw_html"] is not None:
            save(dest / (page["page_id"] + ".html"), page["raw_html"])
    apps = sorted(set(re.findall(r"application_id=(app-[A-Za-z0-9_-]+)", logs)))
    report["application_ids"] = apps
    save(dest / "report.json", json.dumps(report, indent=2))
    for app in apps:
        worker_logs = compose("exec", "-T", "spark-worker", "python", "-c",
            "from pathlib import Path; p=Path('/opt/spark/work/" + app + "'); "
            "[print(str(f)+'\\n'+f.read_text()) for f in sorted(p.rglob('*')) "
            "if f.is_file() and f.name in ('stdout','stderr')]")
        save(dest / ("executor-" + app + ".txt"), worker_logs)
        if "Executor" not in worker_logs or "Running task" not in worker_logs:
            raise RuntimeError("Missing standalone executor work: " + app)
    if not apps:
        raise RuntimeError("No Spark application ID; see task-logs.txt")
    return output, report, logs


def await_run(run):
    def finished():
        state = query("airflow-db", f"SELECT state FROM dag_run WHERE dag_id='{DAG}' AND run_id='{run['airflow_run_id']}'")
        return state if state in ("success", "failed") else None
    run["state"] = wait_until(finished, "day-3 DagRun terminal state", seconds=900)


def check_word_counts(output):
    """Independent host check over saved Silver; Spark still computes all output."""
    for doc in output["documents"]:
        tokens, current = [], []
        for char in unicodedata.normalize("NFC", doc["clean_text"].lower()) + " ":
            category = unicodedata.category(char)
            if category.startswith("L") or category == "Nd":
                current.append(char)
            elif current:
                tokens.append("".join(current))
                current = []
        expected = Counter(tokens)
        actual = {w["word"]: w["count"] for w in output["word_counts"] if w["page_id"] == doc["page_id"]}
        if actual != dict(expected) or len(tokens) != doc["token_count"]:
            raise RuntimeError("Web counts differ from independent count of saved Silver text")


def trigger(urls, top_n=3, *, expect_error=False):
    timestamp = datetime.now(timezone.utc)
    air_id = "day3_" + timestamp.strftime("%Y%m%dT%H%M%S%fZ")
    run_id = str(uuid5(NAMESPACE_URL, json.dumps([DAG, air_id], separators=(",", ":"))))
    run = {"airflow_run_id": air_id, "run_id": run_id, "logical_date": timestamp.replace(microsecond=0).isoformat(),
           "conf": {"urls": urls, "top_n": top_n}, "evidence": EVIDENCE / air_id}
    wait_until(lambda: query("airflow-db", f"SELECT count(*) FROM dag WHERE dag_id='{DAG}'") == "1", "DAG registration")
    scheduler("airflow", "dags", "unpause", DAG)
    scheduler("airflow", "dags", "trigger", DAG, "--run-id", air_id, "--logical-date", run["logical_date"],
              "--conf", json.dumps(run["conf"]))
    print(f"Triggered {air_id}; run_id={run_id}; urls={urls}", flush=True)
    await_run(run)
    output, report, logs = collect(run)
    expected_state = "failed" if expect_error else "success"
    if run["state"] != expected_state:
        raise RuntimeError(f"DAG {run['state']}; expected {expected_state}; see {run['evidence']}")
    if expect_error:
        if not output["bronze"] or not all(p["fetch_error"] for p in output["bronze"]):
            raise RuntimeError("Expected an explicit Bronze fetch_error per invalid URL")
        if output["documents"] or output["word_counts"] or output["metadata"]["publish_status"] != "failed":
            raise RuntimeError("Invalid URL leaked into public output")
        downstream = [t for t in report["tasks"] if t["task_id"] in ("publish_results", "verify_output")]
        if len(downstream) != 2 or any(t["state"] != "upstream_failed" for t in downstream):
            raise RuntimeError("Upstream error concealed by downstream tasks")
        if not all(p["fetch_error"] in logs for p in output["bronze"]):
            raise RuntimeError("Source reason missing from task logs")
        compose("cp", str(ROOT / "tests/test_day2.py"), "airflow-scheduler:/tmp/test_day2.py")
        save(run["evidence"] / "publish-blocked.txt", scheduler("python", "/tmp/test_day2.py", "--assert-blocked", run_id))
    else:
        if len(report["tasks"]) != 5 or any(t["state"] != "success" for t in report["tasks"]):
            raise RuntimeError("Expected all five tasks success")
        scheduler("python", "-m", "src.crawler.pipeline", "verify", "--run-id", run_id, "--top-n", str(top_n))
        check_word_counts(output)
        expected_top = sum(min(top_n, sum(w["page_id"] == d["page_id"] for w in output["word_counts"])) for d in output["documents"])
        if len(output["top_n"]) != expected_top:
            raise RuntimeError("Top-N must be applied per page")
    report["acceptance"] = "PASS"
    save(run["evidence"] / "initial/report.json", json.dumps(report, indent=2))
    print(f"PASS: DAG {run['state']}; evidence={run['evidence'].relative_to(ROOT)}", flush=True)
    return run, output


def accept():
    compose("run", "--rm", "--no-deps", "lab-init", show=True)
    for day in (2, 3):
        compose("cp", str(ROOT / f"tests/test_day{day}.py"), f"airflow-scheduler:/tmp/test_day{day}.py")
        save(EVIDENCE / f"unit-day{day}.txt", scheduler("python", f"/tmp/test_day{day}.py", "-v"))
    save(EVIDENCE / "bronze-transaction-tests.txt", scheduler("python", "/tmp/test_day3.py", "--db"))
    errors = scheduler("airflow", "dags", "list-import-errors", "--output", "json")
    save(EVIDENCE / "dag-import-errors.json", errors)
    if json.loads(errors):
        raise RuntimeError("DAG import errors")
    inventories = {}
    for service in ("airflow-scheduler", "spark-worker"):
        inventories[service] = json.loads(compose("exec", "-T", service, "python", "-m", "src.common.runtime_inventory"))
    if ({k: v for k, v in inventories["airflow-scheduler"].items() if k != "spark_submit"} !=
            {k: v for k, v in inventories["spark-worker"].items() if k != "spark_submit"}):
        raise RuntimeError("Driver/worker inventories differ")
    save(EVIDENCE / "runtime.json", json.dumps(inventories, indent=2))
    image_ids = {}
    for service in ("airflow-scheduler", "spark-worker"):
        container = compose("ps", "-q", service).strip()
        image_ids[service] = command("docker", "inspect", "--format", "{{.Image}}", container).strip()
    save(EVIDENCE / "images.json", json.dumps(image_ids, indent=2))
    before_hashes = source_hashes()
    config = json.loads((ROOT / "config/day3_acceptance.json").read_text(encoding="utf-8"))
    first, first_output = trigger([config["first_url"]], config["top_n"])
    second, _ = trigger([config["second_url"]], config["top_n"])
    if source_hashes() != before_hashes:
        raise RuntimeError("Source changed between configured URLs")
    multi, _ = trigger([config["first_url"], config["second_url"], config["first_url"] + "#duplicate"], 2)
    scheduler("airflow", "tasks", "clear", DAG, "--start-date", first["logical_date"], "--end-date", first["logical_date"], "--yes")
    await_run(first)
    replay, replay_report, replay_logs = collect(first, "replay")
    if first["state"] != "success" or replay != first_output:
        raise RuntimeError("Replayed published web snapshot changed")
    if not all(marker in replay_logs for marker in ("CRAWL_NOOP", "TRANSFORM_NOOP", "PUBLISH_NOOP")):
        raise RuntimeError("Replay did not reuse all persisted stages")
    if any(t["state"] != "success" or t["try_number"] < 2 for t in replay_report["tasks"]):
        raise RuntimeError("Replay did not rerun all five tasks")
    replay_report["acceptance"] = "PASS"
    save(first["evidence"] / "replay/report.json", json.dumps(replay_report, indent=2))
    invalid, _ = trigger([config["error_url"]], config["top_n"], expect_error=True)
    # Actual fixture regression through the expanded five-task DAG.
    from day2 import trigger as fixture_trigger
    fixture, _ = fixture_trigger()
    services = [{k: item.get(k) for k in ("Service", "State", "Health")}
                for item in map(json.loads, filter(str.strip, compose("ps", "--format", "json").splitlines()))]
    if len(services) != 7 or any(s["Health"] != "healthy" for s in services):
        raise RuntimeError("Expected seven healthy services")
    save(EVIDENCE / "latest.json", json.dumps({
        "acceptance": "PASS", "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "day3 web input; day4 quality/quarantine/partial not implemented",
        "first_run": first["run_id"], "second_run": second["run_id"], "multi_run": multi["run_id"],
        "error_run": invalid["run_id"], "fixture_regression_run": fixture["run_id"],
        "urls": config, "source_unchanged_between_urls": True, "published_web_replay_unchanged": True,
        "independent_web_counts": "PASS", "services": services, "image_ids": image_ids,
        "evidence_directories": [r["evidence"].relative_to(ROOT).as_posix() for r in (first, second, multi, invalid, fixture)],
        "source_sha256": source_hashes()}, indent=2))
    print("PASS day 3: two configured web URLs, multi-page/dedup, exact replay, error/no publish, fixture regression.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("migrate", "run", "accept", "inspect"))
    parser.add_argument("--urls", nargs="+")
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    if args.action == "migrate":
        compose("run", "--rm", "--no-deps", "lab-init", show=True)
    elif args.action == "accept":
        accept()
    elif args.action == "run":
        if not args.urls:
            parser.error("run requires --urls URL [URL ...]")
        # Host validates using stdlib module; no dependency installation.
        import sys
        sys.path.insert(0, str(ROOT))
        from src.crawler.config import validate_input
        validate_input({"urls": args.urls, "top_n": args.top_n})
        trigger(args.urls, args.top_n)
    else:
        if not args.run_id:
            parser.error("inspect requires --run-id")
        print(json.dumps(snapshot(args.run_id, args.top_n), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, AssertionError, subprocess.TimeoutExpired) as error:
        raise SystemExit(redact(str(error))) from None
