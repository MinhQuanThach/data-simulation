"""Contract tests in the pinned Linux runtime; optional real-DB transaction tests.

Host runner copies this file into /tmp; tests do not require a test framework.
"""
from contextlib import closing
import json
from pathlib import Path
import sys
import unittest
from uuid import uuid4

from src.common.pipeline import ROOT, output_snapshot, publish_results
from src.common.smoke import connect
from src.common.transform import extract_html, tokenize, RULES_VERSION, TRANSFORM_VERSION
from src.common.quality import load_rules, record_check
from src.common.catalog import pin_catalog, record_lineage
from psycopg2.extras import Json


class TransformTests(unittest.TestCase):
    def test_handwritten_oracle(self):
        expected = json.loads((ROOT / "fixtures/expected.json").read_text(encoding="utf-8"))["valid_vi"]
        title, text, reason = extract_html((ROOT / "fixtures" / expected["file"]).read_text(encoding="utf-8"))
        self.assertEqual((title, text, reason), (expected["title"], expected["clean_text"], ""))
        self.assertEqual(tokenize(text), expected["tokens"])

    def test_empty_and_script_only(self):
        for name in ("empty", "script_only"):
            with self.subTest(name=name):
                self.assertEqual(extract_html((ROOT / f"fixtures/html/{name}.html").read_text())[1:],
                                 ("", "empty_clean_text"))

    def test_unicode_and_token_boundaries(self):
        self.assertEqual(tokenize("CHA\u0300O dữ_liệu ２０２６ 12.5 -- & 💻"),
                         ["chào", "dữ", "liệu", "２０２６", "12", "5"])

    def test_body_entities_and_excluded_elements(self):
        self.assertEqual(extract_html("<html><head><title> A&nbsp; B </title></head>"
                                     "<body>  Chào&nbsp; <b>bạn</b> &amp; 2026"
                                     "<style>bad</style><script>bad</script>"
                                     "<noscript>bad</noscript><template>bad</template></body></html>"),
                         ("A B", "Chào bạn & 2026", ""))

    def test_missing_body_and_punctuation(self):
        self.assertEqual(extract_html("<title>only title</title>")[2], "missing_body")
        self.assertEqual(tokenize("! ... _"), [])


def transaction_checks(source_run):
    """Use a fresh retained test snapshot; never modify a published source run."""
    run_id = str(uuid4())
    attempt = "test-1"
    with closing(connect("CRAWLER")) as conn, conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO bronze.web_pages SELECT %s::uuid,page_id,requested_url,final_url,
            fetched_at,http_status,content_type,content_hash,raw_html,response_encoding,fetch_error
            FROM bronze.web_pages WHERE run_id=%s::uuid""", (run_id, source_run))
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO metadata.pipeline_runs
            (run_id,dag_id,airflow_run_id,transform_version,rules_version,active_attempt,
             staging_complete,publish_status,input_count,total_tokens)
            VALUES (%s::uuid,'day2_transaction_test',%s,%s,%s,%s,false,'running',1,12)""",
            (run_id, run_id, TRANSFORM_VERSION, RULES_VERSION, attempt))
        cur.execute("""INSERT INTO staging.documents SELECT %s,%s,page_id,source_url,content_hash,
            title,clean_text,token_count,processed_at,transform_version FROM silver.documents
            WHERE run_id=%s::uuid""", (run_id, attempt, source_run))
    try:
        publish_results(run_id)
        raise AssertionError("Incomplete staging was published")
    except RuntimeError as error:
        assert "incomplete" in str(error)
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO staging.word_counts SELECT %s,%s,page_id,word,count
            FROM demo.word_counts WHERE run_id=%s::uuid""", (run_id, attempt, source_run))
        cur.execute("UPDATE metadata.pipeline_runs SET staging_complete=true,publish_status='staged' WHERE run_id=%s::uuid", (run_id,))
        rules, digest = load_rules()
        cur.execute('''UPDATE metadata.pipeline_runs SET rules_config=%s,rules_sha256=%s,
            valid_count=1,rejected_count=0,quality_status='valid' WHERE run_id=%s::uuid''', (Json(rules), digest, run_id))
        # This retained transaction fixture copies existing Spark output; it does
        # not claim a new Spark execution. Keep the real originating app ID and
        # label the copy operation explicitly in its test-only lineage.
        pin_catalog(cur, run_id)
        cur.execute('''UPDATE metadata.pipeline_runs SET spark_application_id=(SELECT spark_application_id
            FROM metadata.pipeline_runs WHERE run_id=%s::uuid) WHERE run_id=%s::uuid''', (source_run, run_id))
        record_lineage(cur, run_id, attempt, 'staged', {'silver.documents': 1, 'demo.word_counts': 8,
                                                      'quarantine.rejected_pages': 0})
        cur.execute("UPDATE metadata.lineage SET job_name='test_day2.transaction_snapshot_copy' WHERE run_id=%s::uuid", (run_id,))
        for check in ('unique_keys', 'input_balance', 'staging_invariants', 'has_valid_pages'):
            record_check(cur, run_id, attempt, '@run', check, True, 'transaction fixture', 'valid')

    # Mutate only this fresh test snapshot. Even a stale passing check cannot
    # bypass the publisher's independent SQL invariant validation.
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute('UPDATE metadata.pipeline_runs SET input_count=2,valid_count=2 WHERE run_id=%s::uuid', (run_id,))
    try:
        publish_results(run_id)
        raise AssertionError('Incorrect Bronze input count published')
    except RuntimeError as error:
        assert 'Bronze input count changed' in str(error)
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute('UPDATE metadata.pipeline_runs SET input_count=1,valid_count=1 WHERE run_id=%s::uuid', (run_id,))
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute('''WITH removed AS (DELETE FROM staging.word_counts WHERE run_id=%s RETURNING *)
            INSERT INTO staging.word_counts SELECT run_id,attempt_id,page_id,word,count+1 FROM removed''', (run_id,))
    try:
        publish_results(run_id)
        raise AssertionError('Corrupted counts published')
    except RuntimeError as error:
        assert 'invariant' in str(error)
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute('''WITH removed AS (DELETE FROM staging.word_counts WHERE run_id=%s RETURNING *)
            INSERT INTO staging.word_counts SELECT run_id,attempt_id,page_id,word,count-1 FROM removed''', (run_id,))
        cur.execute('UPDATE metadata.pipeline_runs SET rules_sha256=%s WHERE run_id=%s::uuid', ('0' * 64, run_id))
    try:
        publish_results(run_id)
        raise AssertionError('Changed rules published')
    except RuntimeError as error:
        assert 'Rules changed' in str(error)
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute('UPDATE metadata.pipeline_runs SET rules_sha256=%s WHERE run_id=%s::uuid', (digest, run_id))

    def fail_after_documents():
        # A concurrent reader must not see uncommitted Silver or public counts.
        with closing(connect()) as reader, reader, reader.cursor() as cur:
            cur.execute("SELECT count(*) FROM silver.documents WHERE run_id=%s::uuid", (run_id,))
            assert cur.fetchone()[0] == 0, "Uncommitted Silver became visible"
            cur.execute("SELECT count(*) FROM demo.published_word_counts WHERE run_id=%s::uuid", (run_id,))
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM metadata.lineage WHERE run_id=%s::uuid AND publish_status='published'", (run_id,))
            assert cur.fetchone()[0] == 0, 'Uncommitted lineage became visible'
        raise RuntimeError("injected transaction rollback")

    try:
        publish_results(run_id, _after_documents=fail_after_documents)
        raise AssertionError("Rollback injection did not execute")
    except RuntimeError as error:
        assert str(error) == "injected transaction rollback"
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        for table in ("silver.documents", "demo.word_counts"):
            cur.execute(f"SELECT count(*) FROM {table} WHERE run_id=%s::uuid", (run_id,))
            assert cur.fetchone()[0] == 0, "Transaction leaked a partial result"
    assert output_snapshot(run_id)["metadata"]["publish_status"] == "staged"
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM metadata.lineage WHERE run_id=%s::uuid AND publish_status='staged'", (run_id,))
        assert cur.fetchone()[0] == 3, 'Lineage did not roll back with public tables'
    assert publish_results(run_id)["noop"] is False
    before = output_snapshot(run_id)
    assert publish_results(run_id)["noop"] is True
    assert output_snapshot(run_id) == before, "Publish retry changed the public snapshot"
    assert before["metadata"]["total_tokens"] == 12 and len(before["word_counts"]) == 8
    print(json.dumps({"transaction_test_run_id": run_id, "incomplete_publish_blocked": True,
                      "uncommitted_data_invisible": True, "rollback_both_tables": True,
                      "publish_retry_noop": True, 'corrupted_counts_blocked': True, 'rules_drift_blocked': True,
                      'incorrect_bronze_input_count_blocked': True}))


def assert_blocked(run_id):
    try:
        publish_results(run_id)
        raise AssertionError("Failed staging was published")
    except RuntimeError as error:
        assert "incomplete" in str(error)
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        for table in ("silver.documents", "demo.word_counts", "demo.published_word_counts"):
            cur.execute(f"SELECT count(*) FROM {table} WHERE run_id=%s::uuid", (run_id,))
            assert cur.fetchone()[0] == 0
    print(json.dumps({"run_id": run_id, "publish_blocked": True, "public_rows": 0}))


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--transactions":
        transaction_checks(sys.argv[2])
    elif len(sys.argv) == 3 and sys.argv[1] == "--assert-blocked":
        assert_blocked(sys.argv[2])
    else:
        unittest.main(testRunner=unittest.TextTestRunner(stream=sys.stdout, verbosity=2))
