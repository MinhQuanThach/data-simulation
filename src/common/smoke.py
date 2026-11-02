"""Day-1 fixture setup/readiness/verification; no transformation here."""
import hashlib
import json
import os
from pathlib import Path
from urllib.request import urlopen
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg2

FIXTURE_RUN = "00000000-0000-0000-0000-000000000001"
FIXTURE_URL = "https://fixture.invalid/valid-vi"
FIXTURE_PAGE = hashlib.sha256(FIXTURE_URL.encode()).hexdigest()
FIXTURE_PATH = Path("/opt/lab/fixtures/html/valid_vi.html")


def run_uuid(dag_id, airflow_run_id):
    return str(uuid5(NAMESPACE_URL, json.dumps([dag_id, airflow_run_id], ensure_ascii=False, separators=(",", ":"))))


def connect(role="SPARK"):
    return psycopg2.connect(host=os.environ["LAB_DB_HOST"], port=os.environ.get("LAB_DB_PORT", "5432"),
                            dbname=os.environ["LAB_DB_NAME"], user=os.environ[f"{role}_DB_USER"],
                            password=os.environ[f"{role}_DB_PASSWORD"], connect_timeout=10)


def master_state():
    with urlopen("http://spark-master:8080/json/", timeout=10) as response:
        state = json.load(response)
    workers = [w for w in state["workers"] if w["state"] == "ALIVE"]
    if not workers or sum(w["coresfree"] for w in workers) < 1:
        raise RuntimeError("Spark has no registered ALIVE worker with a free core")
    return state


def prepare_run(dag_id, airflow_run_id):
    master_state()
    run_id = run_uuid(dag_id, airflow_run_id)
    with connect("CRAWLER") as conn, conn.cursor() as cur:
        raw = FIXTURE_PATH.read_bytes()
        cur.execute("""INSERT INTO bronze.web_pages
            VALUES (%s::uuid,%s,%s,%s,now(),200,'text/html; charset=utf-8',%s,%s,'utf-8',NULL)
            ON CONFLICT (run_id,page_id) DO NOTHING""",
            (run_id, FIXTURE_PAGE, FIXTURE_URL, FIXTURE_URL, hashlib.sha256(raw).hexdigest(), raw.decode('utf-8')))
        if cur.rowcount not in (0, 1):
            raise RuntimeError("Unexpected fixture input count")
        cur.execute("SELECT count(*) FROM bronze.web_pages WHERE run_id=%s::uuid", (run_id,))
        if cur.fetchone()[0] != 1:
            raise RuntimeError("Fixture seed missing")
    # Explicit cleanup means a whole-task retry can safely repeat JDBC append.
    with connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM staging.smoke_results WHERE run_id=%s", (run_id,))
    print(f"SMOKE_PREPARED run_id={run_id}; fixture only, no web crawl")
    return run_id


def verify_run(run_id):
    UUID(run_id)
    raw = FIXTURE_PATH.read_bytes()
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""SELECT page_id, html_bytes, content_hash, spark_application_id, executor_python
                       FROM staging.smoke_results WHERE run_id=%s""", (run_id,))
        rows = cur.fetchall()
    if len(rows) != 1:
        raise RuntimeError(f"Expected one staging row, got {len(rows)}")
    page_id, size, digest, app_id, executor_python = rows[0]
    assert page_id == FIXTURE_PAGE and size == len(raw)
    assert digest == hashlib.sha256(raw).hexdigest()
    assert app_id.startswith("app-") and executor_python.startswith("3.11.")
    result = dict(run_id=run_id, page_id=page_id, html_bytes=size, content_hash=digest,
                  spark_application_id=app_id, executor_python=executor_python)
    print("SMOKE_VERIFIED " + json.dumps(result))
    return result
