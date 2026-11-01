"""Host operator runner: migrate / run / accept / inspect. Uses Docker, stdlib only.

accept runs actual Airflow DagRuns, injected task retry, exact-run clear/replay,
empty/script-only rejection, and PostgreSQL transaction rollback/visibility tests.
Evidence/logs are local and redacted with the day-1 helpers; no web crawl.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import re
import subprocess
import time
from uuid import NAMESPACE_URL, UUID, uuid5

from day1 import ROOT, command, compose, query, redact, save, wait_until

DAG = "bronze_to_silver"
TASK = "spark_clean_validate_count"
SOURCE_FILES = (
    "dags/bronze_to_silver.py", "src/jobs/clean_validate_count.py", "src/common/pipeline.py",
    "src/common/transform.py", "sql/migrations/002_day2.sql", "sql/queries/day2_top_n.sql",
    "scripts/init-lab.py", "scripts/day2.py", "tests/test_day2.py", "fixtures/expected.json",
    "fixtures/html/valid_vi.html", "fixtures/html/empty.html", "fixtures/html/script_only.html",
    "compose.yaml", "requirements/airflow.in",
)


def scheduler(*args, **kwargs):
    return compose("exec", "-T", "airflow-scheduler", *args, **kwargs)


def inspect_output(run_id):
    return json.loads(scheduler("python", "-m", "src.common.pipeline", "inspect", "--run-id", str(UUID(run_id))))


def task_states(airflow_run_id):
    return json.loads(query("airflow-db", f"""SELECT coalesce(json_agg(t),'[]') FROM
        (SELECT task_id,state,try_number FROM task_instance WHERE dag_id='{DAG}'
        AND run_id='{airflow_run_id}' ORDER BY task_id) t"""))


def logs(airflow_run_id):
    # IDs are generated locally and constrained before interpolation.
    if not re.fullmatch(r"day2_[A-Za-z0-9_]+", airflow_run_id):
        raise ValueError("Unexpected acceptance DagRun ID")
    return scheduler("python", "-c",
                     "from pathlib import Path; p=Path('/opt/airflow/logs/dag_id=" + DAG + "/run_id=" + airflow_run_id + "'); "
                     "[print(str(f)+'\\n'+f.read_text()) for f in sorted(p.rglob('*.log'))]")


def collect(run, phase="initial"):
    dest = run["evidence"] / phase
    task_logs = logs(run["airflow_run_id"])
    save(dest / "task-logs.txt", task_logs)
    snapshot = inspect_output(run["run_id"])
    save(dest / "sql-output.json", json.dumps(snapshot, ensure_ascii=False, indent=2))
    apps = sorted(set(re.findall(r"application_id=(app-[A-Za-z0-9_-]+)", task_logs)))
    if not apps:
        raise RuntimeError("No Spark application ID in task logs")
    for app_id in apps:
        worker_logs = compose("exec", "-T", "spark-worker", "python", "-c",
                              "from pathlib import Path; p=Path('/opt/spark/work/" + app_id + "'); "
                              "[print(str(f)+'\\n'+f.read_text()) for f in sorted(p.rglob('*')) "
                              "if f.is_file() and f.name in ('stdout','stderr')]")
        save(dest / f"executor-{app_id}.txt", worker_logs)
        if "Executor" not in worker_logs or "Running task" not in worker_logs:
            raise RuntimeError(f"Executor work missing for {app_id}")
    report = {"checked_at_utc": datetime.now(timezone.utc).isoformat(),
              "airflow_run_id": run["airflow_run_id"], "run_id": run["run_id"],
              "logical_date": run["logical_date"], "dag_state": run["state"],
              "tasks": task_states(run["airflow_run_id"]), "application_ids": apps,
              "conf": run["conf"], "scope": "day2 handwritten fixtures, no web crawl",
              "source_sha256": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCE_FILES}}
    save(dest / "report.json", json.dumps(report, indent=2))
    return report, snapshot, task_logs


def await_run(run, observe_retry=False):
    retry_observed = False
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        tasks = task_states(run["airflow_run_id"])
        if observe_retry and not retry_observed and any(t["task_id"] == TASK and t["state"] == "up_for_retry" for t in tasks):
            output = scheduler("python", "/tmp/test_day2.py", "--assert-blocked", run["run_id"])
            snapshot = inspect_output(run["run_id"])
            partial = json.loads(query("lab-db", f"""SELECT json_build_object(
                'documents',(SELECT count(*) FROM staging.documents WHERE run_id='{run['run_id']}'),
                'word_counts',(SELECT count(*) FROM staging.word_counts WHERE run_id='{run['run_id']}'))"""))
            if partial != {"documents": 1, "word_counts": 0} or snapshot["metadata"]["staging_complete"]:
                raise RuntimeError("Expected failed attempt with one partial staging document and no counts")
            save(run["evidence"] / "retry-incomplete.json", json.dumps({"tasks": tasks, "metadata": snapshot["metadata"],
                "partial_staging": partial, "publish_probe": json.loads(output)}, indent=2))
            retry_observed = True
            print(f"Observed real up_for_retry, partial staging hidden, publish refused: {run['run_id']}", flush=True)
        state = query("airflow-db", f"SELECT state FROM dag_run WHERE dag_id='{DAG}' AND run_id='{run['airflow_run_id']}'")
        if state in ("success", "failed"):
            run["state"] = state
            if observe_retry and not retry_observed:
                collect(run)
                raise RuntimeError("Did not observe the injected Airflow task retry")
            return
        time.sleep(3)
    raise RuntimeError("Timeout waiting for day-2 DagRun")


def trigger(case="valid_vi", fail_once=False):
    timestamp = datetime.now(timezone.utc)
    airflow_run_id = "day2_" + timestamp.strftime("%Y%m%dT%H%M%S%fZ")
    run_id = str(uuid5(NAMESPACE_URL, json.dumps([DAG, airflow_run_id], separators=(",", ":"))))
    run = {"airflow_run_id": airflow_run_id, "run_id": run_id, "logical_date": timestamp.replace(microsecond=0).isoformat(),
           "evidence": ROOT / "docs/evidence/day2" / airflow_run_id,
           "conf": {"fixture_case": case, "top_n": 3, "test_fail_after_documents_once": fail_once}}
    wait_until(lambda: query("airflow-db", f"SELECT count(*) FROM dag WHERE dag_id='{DAG}'") == "1", "day-2 DAG registration")
    scheduler("airflow", "dags", "unpause", DAG)
    scheduler("airflow", "dags", "trigger", DAG, "--run-id", airflow_run_id, "--logical-date", run["logical_date"],
              "--conf", json.dumps(run["conf"]))
    print(f"Triggered {airflow_run_id}; run_id={run_id}; case={case}; fail_once={fail_once}", flush=True)
    await_run(run, observe_retry=fail_once)
    report, snapshot, task_logs = collect(run)
    expected_state = "success" if case == "valid_vi" else "failed"
    if run["state"] != expected_state:
        raise RuntimeError(f"DAG {run['state']}; expected {expected_state}; see {run['evidence']}")
    if case == "valid_vi":
        if len(report["tasks"]) != 4 or any(t["state"] != "success" for t in report["tasks"]):
            raise RuntimeError("Expected all four tasks successful")
        scheduler("python", "-m", "src.common.pipeline", "verify", "--run-id", run_id)
        if fail_once:
            task = next(t for t in report["tasks"] if t["task_id"] == TASK)
            if task["try_number"] != 2 or len(report["application_ids"]) != 2:
                raise RuntimeError("Expected two Spark attempts and application IDs")
            if snapshot["metadata"]["active_attempt"] != "spark-2":
                raise RuntimeError("Wrong active staging attempt after retry")
            stale = query("lab-db", f"SELECT count(*) FROM staging.documents WHERE run_id='{run_id}' AND attempt_id<>'spark-2'")
            if stale != "0":
                raise RuntimeError("Stale retry staging survived cleanup")
    else:
        if "empty_clean_text" not in task_logs or snapshot["documents"] or snapshot["word_counts"]:
            raise RuntimeError("Invalid fixture was not rejected before publication")
        if snapshot["metadata"]["staging_complete"] or snapshot["metadata"]["publish_status"] != "failed":
            raise RuntimeError("Invalid fixture was incorrectly marked complete")
        if any(t["task_id"] in ("publish_results", "verify_output") and t["state"] != "upstream_failed" for t in report["tasks"]):
            raise RuntimeError("Downstream task concealed upstream failure")
        scheduler("python", "/tmp/test_day2.py", "--assert-blocked", run_id)
    report["acceptance"] = "PASS"
    save(run["evidence"] / "initial/report.json", json.dumps(report, indent=2))
    print(f"PASS {case}: DAG {run['state']}; evidence={run['evidence'].relative_to(ROOT)}", flush=True)
    return run, snapshot


def accept():
    compose("run", "--rm", "--no-deps", "lab-init", show=True)
    compose("cp", str(ROOT / "tests/test_day2.py"), "airflow-scheduler:/tmp/test_day2.py")
    unit_output = scheduler("python", "/tmp/test_day2.py", "-v")
    import_errors = scheduler("airflow", "dags", "list-import-errors", "--output", "json")
    if json.loads(import_errors):
        raise RuntimeError("DAG import errors")
    normal, _ = trigger()
    retry, before = trigger(fail_once=True)
    # CLI date boundaries are identical and match only this generated logical date.
    scheduler("airflow", "tasks", "clear", DAG, "--start-date", retry["logical_date"],
              "--end-date", retry["logical_date"], "--yes")
    await_run(retry)
    replay_report, after, replay_logs = collect(retry, "replay")
    if retry["state"] != "success" or before != after:
        raise RuntimeError("Clearing/replaying the published DagRun changed the snapshot")
    if "TRANSFORM_NOOP" not in replay_logs or "PUBLISH_NOOP" not in replay_logs:
        raise RuntimeError("Replay did not exercise transform/publish no-op")
    if any(t["state"] != "success" or t["try_number"] < 2 for t in replay_report["tasks"]):
        raise RuntimeError("Exact-run task clear did not replay all four tasks")
    replay_report["acceptance"] = "PASS"
    save(retry["evidence"] / "replay/report.json", json.dumps(replay_report, indent=2))
    print("PASS published DagRun clear/replay: exact same snapshot and timestamps", flush=True)
    transaction_output = scheduler("python", "/tmp/test_day2.py", "--transactions", normal["run_id"])
    save(normal["evidence"] / "transaction-tests.txt", transaction_output)
    empty, _ = trigger("empty")
    script_only, _ = trigger("script_only")
    services = [{k: item.get(k) for k in ("Service", "State", "Health")}
                for item in map(json.loads, filter(str.strip, compose("ps", "--format", "json").splitlines()))]
    if len(services) != 7 or any(s["Health"] != "healthy" for s in services):
        raise RuntimeError("Expected seven healthy services after acceptance")
    summary = {"acceptance": "PASS", "checked_at_utc": datetime.now(timezone.utc).isoformat(),
               "scope": "day2 fixture only; day3+ not accepted", "services": services,
               "normal_run": normal["run_id"], "retry_and_replay_run": retry["run_id"],
               "empty_run": empty["run_id"], "script_only_run": script_only["run_id"],
               "publish_rollback_and_visibility": "PASS", "published_replay_unchanged": True,
               "evidence_directories": [r["evidence"].relative_to(ROOT).as_posix() for r in (normal, retry, empty, script_only)],
               "source_sha256": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCE_FILES}}
    save(ROOT / "docs/evidence/day2/unit-tests.txt", unit_output)
    save(ROOT / "docs/evidence/day2/latest.json", json.dumps(summary, indent=2))
    print("PASS day 2: oracle, standalone executors, real task retry, published replay, transaction rollback, invalid fixtures.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("migrate", "run", "accept", "inspect"))
    parser.add_argument("--run-id")
    args = parser.parse_args()
    if args.action == "migrate":
        compose("run", "--rm", "--no-deps", "lab-init", show=True)
    elif args.action == "accept":
        accept()
    elif args.action == "run":
        trigger()
    else:
        if not args.run_id:
            parser.error("inspect requires --run-id")
        print(json.dumps(inspect_output(args.run_id), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, AssertionError, subprocess.TimeoutExpired) as error:
        raise SystemExit(redact(str(error))) from None
