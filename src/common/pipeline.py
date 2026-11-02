"""Day-2 fixture orchestration and atomic publish; no web requests to fixtures."""
import argparse
from contextlib import closing, contextmanager
import hashlib
import json
from pathlib import Path
from uuid import UUID
from psycopg2.extras import Json

from src.common.smoke import connect, master_state, run_uuid
from src.common.transform import RULES_VERSION, TRANSFORM_VERSION
from src.common.quality import load_rules, quality_snapshot
from src.common.catalog import pin_catalog, publish_lineage

ROOT = Path(__file__).resolve().parents[2]
PUBLISHED = "published"


@contextmanager
def locked_run(run_id, role='SPARK'):
    """Same session lock for Spark attempts and publisher, held across commits.

    Closing the session releases the lock even if an attempt raises. JDBC uses
    separate sessions, so only control operations take this advisory lock.
    """
    run_id = str(UUID(run_id))
    with closing(connect(role)) as conn:
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '30s'")
            cur.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (run_id,))
        conn.commit()
        yield conn


def check_versions(row):
    historical = row is not None and len(row) > 2 and row[2] == PUBLISHED and tuple(row[:2]) == ('1.0.0', '1.0.0')
    if not historical and (row is None or tuple(row[:2]) != (TRANSFORM_VERSION, RULES_VERSION)):
        raise RuntimeError("Missing pipeline run or incompatible transform/rules versions")


def prepare_fixture(dag_id, airflow_run_id, **context):
    from src.crawler.config import validate_input
    config = validate_input(context['dag_run'].conf or {})
    case, top_n, fail_once = config['fixture_case'], config['top_n'], config['fail_once']
    master_state()
    run_id = run_uuid(dag_id, airflow_run_id)
    cases = {'mixed': ['valid_vi', 'empty', 'script_only', 'http_error', 'missing_body'],
             'all_invalid': ['empty', 'script_only', 'http_error', 'missing_body', 'no_tokens']}.get(case, [case])
    with locked_run(run_id) as conn, conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO metadata.pipeline_runs
            (run_id,dag_id,airflow_run_id,transform_version,rules_version,input_config)
            VALUES (%s::uuid,%s,%s,%s,%s,%s) ON CONFLICT (run_id) DO NOTHING""",
            (run_id, dag_id, airflow_run_id, TRANSFORM_VERSION, RULES_VERSION, Json(config)))
        cur.execute("SELECT transform_version,rules_version,publish_status,input_config FROM metadata.pipeline_runs WHERE run_id=%s::uuid", (run_id,))
        row = cur.fetchone()
        check_versions(row)
        pin_catalog(cur, run_id)
        if row[3] is not None and row[3] != config:
            raise RuntimeError('Snapshot configuration changed; create a new DagRun')
        with closing(connect('CRAWLER')) as bronze_conn, bronze_conn, bronze_conn.cursor() as writer:
            expected = []
            for item in cases:
                url = 'https://fixture.invalid/' + item.replace('_', '-')
                page = hashlib.sha256(url.encode()).hexdigest()
                filename = 'valid_vi' if item in ('http_error', 'unsupported_type') else item
                raw = (ROOT / f'fixtures/html/{filename}.html').read_bytes()
                digest = hashlib.sha256(raw).hexdigest()
                status, error = (404, 'http_404') if item == 'http_error' else (200, None)
                content_type = 'application/pdf' if item == 'unsupported_type' else 'text/html; charset=utf-8'
                writer.execute('''INSERT INTO bronze.web_pages VALUES
                    (%s::uuid,%s,%s,%s,now(),%s,%s,%s,%s,'utf-8',%s)
                    ON CONFLICT (run_id,page_id) DO NOTHING''',
                    (run_id, page, url, url, status, content_type, digest, raw.decode('utf-8'), error))
                expected.append((page, digest))
            writer.execute('SELECT page_id,content_hash FROM bronze.web_pages WHERE run_id=%s::uuid ORDER BY page_id', (run_id,))
            if writer.fetchall() != sorted(expected):
                raise RuntimeError('Existing Bronze snapshot differs from requested fixture')
    print(f"FIXTURE_PREPARED run_id={run_id} case={case}; handwritten, no web crawl")
    return {"run_id": run_id, "top_n": top_n, "fixture_case": case, "fail_once": fail_once,
            'fail_always': config['fail_always']}


def validate_staging(cur, run_id, attempt_id, input_count, total_tokens, rejected_count=0):
    valid_count = input_count - rejected_count
    cur.execute('SELECT count(*) FROM bronze.web_pages WHERE run_id=%s::uuid', (run_id,))
    if cur.fetchone()[0] != input_count:
        raise RuntimeError('Staging invariant failed: Bronze input count changed')
    cur.execute("""SELECT d.page_id,d.token_count,coalesce(sum(w.count),0)
        FROM staging.documents d LEFT JOIN staging.word_counts w
        USING (run_id,attempt_id,page_id)
        WHERE d.run_id=%s AND d.attempt_id=%s
        GROUP BY d.page_id,d.token_count""", (run_id, attempt_id))
    rows = cur.fetchall()
    if not rows or len(rows) != valid_count or any(tokens <= 0 or tokens != counts for _, tokens, counts in rows):
        raise RuntimeError("Staging invariant failed: input/doc count or per-page token sum")
    if sum(row[1] for row in rows) != total_tokens:
        raise RuntimeError("Staging total_tokens mismatch")
    cur.execute("""SELECT count(*) FROM staging.documents d
        JOIN bronze.web_pages b ON b.run_id=d.run_id::uuid AND b.page_id=d.page_id
        WHERE d.run_id=%s AND d.attempt_id=%s AND d.content_hash=b.content_hash
        AND d.source_url=coalesce(b.final_url,b.requested_url) AND d.transform_version=%s""",
        (run_id, attempt_id, TRANSFORM_VERSION))
    if cur.fetchone()[0] != valid_count:
        raise RuntimeError("Staging does not match Bronze source/version")
    cur.execute("SELECT rules_version FROM metadata.pipeline_runs WHERE run_id=%s::uuid", (run_id,))
    if cur.fetchone()[0] == RULES_VERSION:
        cur.execute('''SELECT count(*),count(*) FILTER (WHERE q.attempt_id=%s),
            count(*) FILTER (WHERE EXISTS (SELECT 1 FROM staging.documents d
                WHERE d.run_id=%s AND d.attempt_id=%s AND d.page_id=q.page_id))
            FROM quarantine.rejected_pages q WHERE q.run_id=%s::uuid''',
            (attempt_id, run_id, attempt_id, run_id))
        if cur.fetchone() != (rejected_count, rejected_count, 0):
            raise RuntimeError('Quarantine count/attempt or accepted/rejected overlap')
        cur.execute('''SELECT count(*) FROM bronze.web_pages b WHERE b.run_id=%s::uuid
            AND (EXISTS (SELECT 1 FROM staging.documents d WHERE d.run_id=%s AND d.attempt_id=%s AND d.page_id=b.page_id)
                 OR EXISTS (SELECT 1 FROM quarantine.rejected_pages q WHERE q.run_id=b.run_id AND q.page_id=b.page_id))''',
            (run_id, run_id, attempt_id))
        if cur.fetchone()[0] != input_count:
            raise RuntimeError('Input must equal valid plus rejected')


def publish_results(run_id, *, _after_documents=None):
    """All public tables + published flag commit together. Hook is for rollback tests."""
    run_id = str(UUID(run_id))
    with locked_run(run_id, 'PUBLISHER') as conn, conn, conn.cursor() as cur:
        cur.execute("""SELECT transform_version,rules_version,publish_status,active_attempt,
            staging_complete,input_count,total_tokens,rejected_count,valid_count,quality_status,rules_sha256 FROM metadata.pipeline_runs
            WHERE run_id=%s::uuid FOR UPDATE""", (run_id,))
        row = cur.fetchone()
        check_versions(row)
        _, _, status, attempt, complete, input_count, total_tokens, rejected, valid, quality_status, rules_hash = row
        if status == PUBLISHED:
            if not complete or not attempt:
                raise RuntimeError("Inconsistent published run")
            print(f"PUBLISH_NOOP run_id={run_id} attempt_id={attempt}")
            return {"run_id": run_id, "status": PUBLISHED, "noop": True}
        if status != "staged" or not complete or not attempt:
            raise RuntimeError("Cannot publish incomplete or failed Spark staging")
        if row[1] == RULES_VERSION:
            if quality_status not in ('valid', 'partial') or valid != input_count - rejected or valid <= 0:
                raise RuntimeError('Quality gate is incomplete or failed')
            if rules_hash != load_rules()[1]:
                raise RuntimeError('Rules changed; create a new DagRun')
            cur.execute('''SELECT check_name,status FROM quality.check_results WHERE run_id=%s::uuid
                AND attempt_id=%s AND scope_key='@run' ''', (run_id, attempt))
            checks = dict(cur.fetchall())
            required = {'unique_keys', 'input_balance', 'staging_invariants', 'has_valid_pages'}
            if not required <= checks.keys() or any(checks[n] != 'pass' for n in required):
                raise RuntimeError('Quality run checks failed or missing')
        validate_staging(cur, run_id, attempt, input_count, total_tokens, rejected or 0)
        publish_lineage(cur, run_id, attempt)
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
        diagnostics = quality_snapshot(cur, run_id)
    # Display limit only. The complete counts above remain available for checks.
    displayed = []
    for page in dict.fromkeys(row["page_id"] for row in counts):
        displayed.extend([row for row in counts if row["page_id"] == page][:top_n])
    return {"metadata": metadata, "documents": documents, "word_counts": counts,
            "top_n": displayed, **diagnostics}


def verify_output(run_id, top_n=3):
    snapshot = output_snapshot(run_id, int(top_n))
    expected = json.loads((ROOT / "fixtures/expected.json").read_text(encoding="utf-8"))["valid_vi"]
    docs, counts, meta = snapshot["documents"], snapshot["word_counts"], snapshot["metadata"]
    if meta["publish_status"] != PUBLISHED or len(docs) != 1 or meta["input_count"] != 1 + len(snapshot['quarantine']):
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
