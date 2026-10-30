"""Run from the repository host: up / smoke / status / stop. Stdlib only.

Docker CLI calls here are operator commands, never executed inside a DAG.
Evidence contains selected fields and redacted logs; .env is never printed.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
ENV = {}
if (ROOT / ".env").exists():
    ENV = dict(line.split("=", 1) for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines()
               if line and not line.startswith("#"))


def redact(value):
    for key, secret in ENV.items():
        if any(part in key for part in ("PASSWORD", "SECRET", "FERNET")) and secret:
            value = value.replace(secret, "[REDACTED]")
    return value


def command(*args, timeout=900, show=False):
    result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
    output = redact(result.stdout)
    if show or result.returncode:
        print(output, end="")
        print(redact(result.stderr), end="", file=sys.stderr)
    if result.returncode:
        raise RuntimeError(f"Command failed: {args[0]} (exit {result.returncode})")
    return output


def compose(*args, **kwargs):
    return command("docker", "compose", *args, **kwargs)


def query(service, sql):
    prefix = "AIRFLOW" if service == "airflow-db" else "LAB_ADMIN"
    username = ENV.get("AIRFLOW_DB_USER", "airflow") if prefix == "AIRFLOW" else ENV.get("LAB_ADMIN_USER", "lab_admin")
    database = ENV.get("AIRFLOW_DB_NAME", "airflow") if prefix == "AIRFLOW" else ENV.get("LAB_DB_NAME", "lab")
    return compose("exec", "-T", service, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", username, "-d", database, "-At", "-c", sql).strip()


def wait_until(probe, description, seconds=180):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = probe()
        if value:
            return value
        time.sleep(5)
    raise RuntimeError(f"Timeout waiting for {description}")


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(redact(value), encoding="utf-8", newline="\n")


def smoke():
    from uuid import NAMESPACE_URL, uuid5
    airflow_run_id = "day1_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_id = str(uuid5(NAMESPACE_URL, json.dumps(["smoke_bronze", airflow_run_id], separators=(",", ":"))))
    evidence = ROOT / "docs/evidence/day1" / airflow_run_id
    wait_until(lambda: query("airflow-db", "SELECT count(*) FROM dag WHERE dag_id='smoke_bronze'") == "1", "DAG registration")
    compose("exec", "-T", "airflow-scheduler", "airflow", "dags", "unpause", "smoke_bronze", show=True)
    compose("exec", "-T", "airflow-scheduler", "airflow", "dags", "trigger", "smoke_bronze", "--run-id", airflow_run_id, show=True)
    print(f"Waiting for scheduled DAG run {airflow_run_id}; run_id={run_id}", flush=True)

    def finished():
        state = query("airflow-db", f"SELECT state FROM dag_run WHERE dag_id='smoke_bronze' AND run_id='{airflow_run_id}'")
        return state if state in ("success", "failed") else None

    state = wait_until(finished, "DAG terminal state", seconds=600)
    tasks = query("airflow-db", f"SELECT coalesce(json_agg(t),'[]') FROM (SELECT task_id,state,try_number FROM task_instance WHERE dag_id='smoke_bronze' AND run_id='{airflow_run_id}' ORDER BY task_id) t")
    services = [json.loads(line) for line in compose("ps", "--format", "json").splitlines() if line.strip()]
    service_states = [{key: item.get(key) for key in ("Service", "State", "Health")} for item in services]
    report = {"checked_at_utc": datetime.now(timezone.utc).isoformat(), "scope": "day1 handwritten fixture; no web crawl or Silver publish",
              "airflow_run_id": airflow_run_id, "run_id": run_id, "dag_state": state, "tasks": json.loads(tasks), "services": service_states}
    report["source_sha256"] = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in (
        "compose.yaml", "docker/airflow/Dockerfile", "requirements/airflow.in",
        "docker/airflow/fetch_jdbc.py", "docker/airflow/fetch_pyspark.py",
        "requirements/constraints-project-py311.txt", "config/stack.json",
        "dags/smoke_bronze.py", "src/common/smoke.py", "src/jobs/smoke_jdbc.py", "sql/init/001_day1.sql")}
    save(evidence / "report.json", json.dumps(report, indent=2))
    task_logs = compose("exec", "-T", "airflow-scheduler", "python", "-c",
                        "from pathlib import Path; p=Path('/opt/airflow/logs/dag_id=smoke_bronze/run_id=" + airflow_run_id + "'); [print(f.read_text()) for f in sorted(p.rglob('*.log'))]")
    save(evidence / "task-logs.txt", task_logs)
    if state != "success":
        raise RuntimeError(f"DAG {state}; see {evidence / 'task-logs.txt'}")
    if len(service_states) != 7 or any(s["Health"] != "healthy" for s in service_states):
        raise RuntimeError("Expected seven healthy long-running services")
    if len(report["tasks"]) != 3 or any(t["state"] != "success" for t in report["tasks"]):
        raise RuntimeError("Expected all three task instances to succeed")
    output = query("lab-db", f"SELECT json_agg(t) FROM (SELECT * FROM staging.smoke_results WHERE run_id='{run_id}') t")
    rows = json.loads(output)
    assert len(rows) == 1, "Expected exactly one staging output"
    raw = (ROOT / "fixtures/html/valid_vi.html").read_bytes()
    assert rows[0]["html_bytes"] == len(raw) and rows[0]["content_hash"] == hashlib.sha256(raw).hexdigest()
    app_id = rows[0]["spark_application_id"]
    assert re.fullmatch(r"app-[A-Za-z0-9_-]+", app_id)
    assert app_id in task_logs, "Spark application ID missing from Airflow task logs"
    worker_logs = compose("exec", "-T", "spark-worker", "python", "-c",
                          "from pathlib import Path; p=Path('/opt/spark/work/" + app_id + "'); [print(str(f)+'\\n'+f.read_text()) for f in sorted(p.rglob('*')) if f.is_file() and f.name in ('stdout','stderr')]")
    assert "Executor" in worker_logs and "Running task" in worker_logs, "Executor work log missing"
    save(evidence / "executor-logs.txt", worker_logs)
    save(evidence / "sql-output.json", json.dumps(rows, indent=2))
    state_json = compose("exec", "-T", "airflow-scheduler", "python", "-c",
                         "import json; from src.common.smoke import master_state; print(json.dumps(master_state()))")
    save(evidence / "spark-master.json", state_json)
    report.update(result=rows[0], acceptance="PASS")
    save(evidence / "report.json", json.dumps(report, indent=2))
    save(ROOT / "docs/evidence/day1/latest.json", json.dumps({"evidence_directory": evidence.relative_to(ROOT).as_posix(), "run_id": run_id, "application_id": app_id, "acceptance": "PASS"}, indent=2))
    print(f"PASS: seven healthy services, three successful tasks, standalone executor log, fixture JDBC output. Evidence: {evidence.relative_to(ROOT)}")


def runtime():
    evidence = ROOT / "docs/evidence/day1"
    inventories = {}
    for service in ("airflow-scheduler", "spark-worker"):
        inventories[service] = json.loads(compose("exec", "-T", service, "python", "-m", "src.common.runtime_inventory"))
    comparable = lambda info: {key: value for key, value in info.items() if key != "spark_submit"}
    assert comparable(inventories["airflow-scheduler"]) == comparable(inventories["spark-worker"]), "Driver and worker runtime mismatch"
    inventories["custom_image_id"] = command("docker", "image", "inspect", "bronze-silver-runtime:day1", "--format", "{{.Id}}").strip()
    inventories["custom_image_repo_digests"] = json.loads(command("docker", "image", "inspect", "bronze-silver-runtime:day1", "--format", "{{json .RepoDigests}}"))
    save(evidence / "runtime.json", json.dumps(inventories, indent=2))
    freeze = compose("exec", "-T", "airflow-scheduler", "cat", "/opt/airflow/runtime-freeze.txt")
    save(ROOT / "requirements/runtime-freeze-py311.txt", freeze)
    print("PASS: pip check, provider import, Java/Spark, identical driver/worker runtime, JDBC checksum. Inventory saved.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["up", "smoke", "runtime", "status", "stop"])
    args = parser.parse_args()
    if not ENV:
        raise SystemExit("Generate local secrets first: python scripts/init-env.py")
    if args.action == "up":
        compose("config", "--quiet")
        pins = json.loads((ROOT / "config/stack.json").read_text())
        for name in ("airflow", "spark"):
            pin = pins["images"][name]
            command("docker", "pull", "--platform", "linux/amd64", pin["tag"] + "@" + pin["digest"], show=True)
        compose("pull", "airflow-db", "lab-db", show=True)
        compose("--progress", "plain", "build", "airflow-scheduler", timeout=2400, show=True)
        compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "360", timeout=600, show=True)
    elif args.action == "smoke":
        smoke()
    elif args.action == "runtime":
        runtime()
    elif args.action == "status":
        compose("ps", "-a", show=True)
    else:
        compose("stop", show=True)
        print("Stopped services; database/auth/log/work volumes retained.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.TimeoutExpired, AssertionError) as error:
        raise SystemExit(redact(str(error))) from None
