"""Day-2 fixture orchestration and atomic publish; no web requests to fixtures."""
import argparse
from contextlib import closing, contextmanager
import hashlib
import json
from pathlib import Path
from uuid import UUID

from src.common.smoke import connect, master_state, run_uuid
from src.common.transform import RULES_VERSION, TRANSFORM_VERSION

ROOT = Path(__file__).resolve().parents[2]
PUBLISHED = "published"


@contextmanager
def locked_run(run_id):
    """Same session lock for Spark attempts and publisher, held across commits.

    Closing the session releases the lock even if an attempt raises. JDBC uses
    separate sessions, so only control operations take this advisory lock.
    """
    run_id = str(UUID(run_id))
    with closing(connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '30s'")
            cur.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (run_id,))
        conn.commit()
        yield conn


def check_versions(row):
    if row is None or tuple(row[:2]) != (TRANSFORM_VERSION, RULES_VERSION):
        raise RuntimeError("Missing pipeline run or incompatible transform/rules versions")


def prepare_fixture(dag_id, airflow_run_id, **context):
    conf = context["dag_run"].conf or {}
    case = conf.get("fixture_case", "valid_vi")
    top_n = conf.get("top_n", 3)
    fail_once = conf.get("test_fail_after_documents_once", False)
    if case not in ("valid_vi", "empty", "script_only"):
        raise ValueError("fixture_case must be valid_vi, empty or script_only")
    if type(top_n) is not int or not 1 <= top_n <= 100:
        raise ValueError("top_n must be an integer in 1..100")
    if type(fail_once) is not bool:
        raise ValueError("test_fail_after_documents_once must be boolean")
    master_state()
    run_id = run_uuid(dag_id, airflow_run_id)
    url = "https://fixture.invalid/" + case.replace("_", "-")
    page_id = hashlib.sha256(url.encode()).hexdigest()
    raw = (ROOT / f"fixtures/html/{case}.html").read_bytes()
    with closing(connect("CRAWLER")) as conn, conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO bronze.web_pages VALUES
            (%s::uuid,%s,%s,%s,now(),200,'text/html; charset=utf-8',%s,%s,'utf-8',NULL)
            ON CONFLICT (run_id,page_id) DO NOTHING""",
            (run_id, page_id, url, url, hashlib.sha256(raw).hexdigest(), raw.decode("utf-8")))
        cur.execute("SELECT page_id,content_hash FROM bronze.web_pages WHERE run_id=%s::uuid", (run_id,))
        if cur.fetchall() != [(page_id, hashlib.sha256(raw).hexdigest())]:
            raise RuntimeError("Existing Bronze snapshot differs from requested fixture")
    with locked_run(run_id) as conn, conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO metadata.pipeline_runs
            (run_id,dag_id,airflow_run_id,transform_version,rules_version)
            VALUES (%s::uuid,%s,%s,%s,%s) ON CONFLICT (run_id) DO NOTHING""",
            (run_id, dag_id, airflow_run_id, TRANSFORM_VERSION, RULES_VERSION))
        cur.execute("SELECT transform_version,rules_version FROM metadata.pipeline_runs WHERE run_id=%s::uuid", (run_id,))
        check_versions(cur.fetchone())
    print(f"FIXTURE_PREPARED run_id={run_id} case={case}; handwritten, no web crawl")
    return {"run_id": run_id, "top_n": top_n, "fixture_case": case, "fail_once": fail_once}


def validate_staging(cur, run_id, attempt_id, input_count, total_tokens):
    cur.execute("""SELECT d.page_id,d.token_count,coalesce(sum(w.count),0)
        FROM staging.documents d LEFT JOIN staging.word_counts w
        USING (run_id,attempt_id,page_id)
        WHERE d.run_id=%s AND d.attempt_id=%s
        GROUP BY d.page_id,d.token_count""", (run_id, attempt_id))
    rows = cur.fetchall()
    if not rows or len(rows) != input_count or any(tokens <= 0 or tokens != counts for _, tokens, counts in rows):
        raise RuntimeError("Staging invariant failed: input/doc count or per-page token sum")
    if sum(row[1] for row in rows) != total_tokens:
        raise RuntimeError("Staging total_tokens mismatch")
    cur.execute("""SELECT count(*) FROM staging.documents d
        JOIN bronze.web_pages b ON b.run_id=d.run_id::uuid AND b.page_id=d.page_id
        WHERE d.run_id=%s AND d.attempt_id=%s AND d.content_hash=b.content_hash
        AND d.source_url=coalesce(b.final_url,b.requested_url) AND d.transform_version=%s""",
        (run_id, attempt_id, TRANSFORM_VERSION))
    if cur.fetchone()[0] != input_count:
        raise RuntimeError("Staging does not match Bronze source/version")


def publish_results(run_id, *, _after_documents=None):
    """All public tables + published flag commit together. Hook is for rollback tests."""
    run_id = str(UUID(run_id))
    with locked_run(run_id) as conn, conn, conn.cursor() as cur:
        cur.execute("""SELECT transform_version,rules_version,publish_status,active_attempt,
            staging_complete,input_count,total_tokens FROM metadata.pipeline_runs
            WHERE run_id=%s::uuid FOR UPDATE""", (run_id,))
        row = cur.fetchone()
        check_versions(row)
        _, _, status, attempt, complete, input_count, total_tokens = row
        if status == PUBLISHED:
            if not complete or not attempt:
                raise RuntimeError("Inconsistent published run")
            print(f"PUBLISH_NOOP run_id={run_id} attempt_id={attempt}")
            return {"run_id": run_id, "status": PUBLISHED, "noop": True}
        if status != "staged" or not complete or not attempt:
            raise RuntimeError("Cannot publish incomplete or failed Spark staging")
        validate_staging(cur, run_id, attempt, input_count, total_tokens)
        # FK order matters. Deletes are scoped to this snapshot only.
        cur.execute("DELETE FROM demo.word_counts WHERE run_id=%s::uuid", (run_id,))
        cur.execute("DELETE FROM silver.documents WHERE run_id=%s::uuid", (run_id,))
        cur.execute("""INSERT INTO silver.documents
            SELECT run_id::uuid,page_id,source_url,content_hash,title,clean_text,
                token_count,processed_at,transform_version FROM staging.documents
            WHERE run_id=%s AND attempt_id=%s""", (run_id, attempt))
        if _after_documents is not None:
            _after_documents()
        cur.execute("""INSERT INTO demo.word_counts
            SELECT run_id::uuid,page_id,word,count FROM staging.word_counts
            WHERE run_id=%s AND attempt_id=%s""", (run_id, attempt))
        cur.execute("""UPDATE metadata.pipeline_runs SET publish_status='published',
            published_at=now(),error_summary=NULL WHERE run_id=%s::uuid""", (run_id,))
    print(f"PUBLISH_SUCCESS run_id={run_id} attempt_id={attempt} total_tokens={total_tokens}")
    return {"run_id": run_id, "status": PUBLISHED, "noop": False}


def output_snapshot(run_id, top_n=3):
    run_id = str(UUID(run_id))
    if type(top_n) is not int or not 1 <= top_n <= 100:
        raise ValueError("top_n must be an integer in 1..100")
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute("SELECT row_to_json(r) FROM metadata.pipeline_runs r WHERE run_id=%s::uuid", (run_id,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Pipeline run not found")
        metadata = row[0]
        cur.execute("""SELECT row_to_json(d) FROM silver.documents d
            JOIN metadata.pipeline_runs r USING (run_id)
            WHERE d.run_id=%s::uuid AND r.publish_status='published' ORDER BY d.page_id""", (run_id,))
        documents = [row[0] for row in cur.fetchall()]
        cur.execute("""SELECT page_id,word,count FROM demo.published_word_counts
            WHERE run_id=%s::uuid ORDER BY page_id,count DESC,word COLLATE "C" ASC""", (run_id,))
        counts = [{"page_id": page, "word": word, "count": count} for page, word, count in cur.fetchall()]
    # Display limit only. The complete counts above remain available for checks.
    return {"metadata": metadata, "documents": documents, "word_counts": counts,
            "top_n": counts[:top_n]}


def verify_output(run_id, top_n=3):
    snapshot = output_snapshot(run_id, int(top_n))
    expected = json.loads((ROOT / "fixtures/expected.json").read_text(encoding="utf-8"))["valid_vi"]
    docs, counts, meta = snapshot["documents"], snapshot["word_counts"], snapshot["metadata"]
    if meta["publish_status"] != PUBLISHED or len(docs) != 1 or meta["input_count"] != 1:
        raise RuntimeError("Expected one published fixture document")
    doc = docs[0]
    for field in ("title", "clean_text", "token_count"):
        if doc[field] != expected[field]:
            raise RuntimeError(f"Fixture mismatch: {field}")
    raw = (ROOT / "fixtures" / expected["file"]).read_bytes()
    if doc["source_url"] != expected["requested_url"] or doc["content_hash"] != hashlib.sha256(raw).hexdigest():
        raise RuntimeError("Fixture source/hash mismatch")
    if doc["page_id"] != hashlib.sha256(expected["requested_url"].encode()).hexdigest():
        raise RuntimeError("Fixture page_id mismatch")
    if {row["word"]: row["count"] for row in counts} != expected["word_counts"] or len(counts) != 8:
        raise RuntimeError("Complete word counts differ from handwritten oracle")
    if sum(row["count"] for row in counts) != doc["token_count"] or meta["total_tokens"] != doc["token_count"]:
        raise RuntimeError("Published token sum mismatch")
    top = [{key: row[key] for key in ("word", "count")} for row in counts[:3]]
    if top != expected["top_3"]:
        raise RuntimeError("Top-3 ordering mismatch")
    result = {"run_id": run_id, "token_count": doc["token_count"], "distinct_words": len(counts),
              "application_id": meta["spark_application_id"], "top_n": int(top_n)}
    print("FIXTURE_VERIFIED " + json.dumps(result, ensure_ascii=False))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("inspect", "publish", "verify"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--top-n", type=int, default=3)
    args = parser.parse_args()
    if args.action == "publish":
        publish_results(args.run_id)
    elif args.action == "verify":
        verify_output(args.run_id, args.top_n)
    else:
        print(json.dumps(output_snapshot(args.run_id, args.top_n), ensure_ascii=False, indent=2))
