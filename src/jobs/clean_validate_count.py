"""Real standalone Spark transform and complete per-page word counts over JDBC."""
import argparse
import os
import re
from uuid import UUID
from psycopg2.extras import Json

from pyspark.sql import SparkSession, functions as F

from src.common.pipeline import check_versions, locked_run, validate_staging
from src.common.smoke import master_state
from src.common.transform import TRANSFORM_VERSION
from src.common.quality import classify, load_rules, record_check
from src.common.catalog import pin_catalog, record_lineage


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--fail-after-documents-once", choices=("true", "false"), default="false")
    parser.add_argument("--fail-after-documents-always", choices=("true", "false"), default="false")
    args = parser.parse_args()
    run_id = str(UUID(args.run_id))
    attempt = args.attempt_id
    if not re.fullmatch(r"spark-[1-9][0-9]*", attempt):
        raise ValueError("attempt-id must be spark-<Airflow try_number>")
    spark = None
    with locked_run(run_id) as conn:
        with conn, conn.cursor() as cur:
            cur.execute("""SELECT transform_version,rules_version,publish_status
                FROM metadata.pipeline_runs WHERE run_id=%s::uuid FOR UPDATE""", (run_id,))
            row = cur.fetchone()
            check_versions(row)
            if row[2] == "published":
                print(f"TRANSFORM_NOOP run_id={run_id}; snapshot already published")
                return
            cur.execute("DELETE FROM staging.word_counts WHERE run_id=%s", (run_id,))
            cur.execute("DELETE FROM staging.documents WHERE run_id=%s", (run_id,))
            cur.execute('DELETE FROM quarantine.rejected_pages WHERE run_id=%s::uuid', (run_id,))
            cur.execute("""UPDATE metadata.pipeline_runs SET active_attempt=%s,
                staging_complete=false,publish_status='running',spark_application_id=NULL,
                input_count=NULL,total_tokens=NULL,valid_count=NULL,rejected_count=NULL,
                quality_status='pending',error_summary=NULL WHERE run_id=%s::uuid""", (attempt, run_id))
        try:
            rules, rules_hash = load_rules()
            with conn, conn.cursor() as cur:
                pin_catalog(cur, run_id)
                cur.execute('SELECT rules_sha256 FROM metadata.pipeline_runs WHERE run_id=%s::uuid', (run_id,))
                old_hash = cur.fetchone()[0]
                if old_hash is not None and old_hash != rules_hash:
                    raise RuntimeError('Rules changed; create a new DagRun')
                cur.execute('UPDATE metadata.pipeline_runs SET rules_config=%s,rules_sha256=%s WHERE run_id=%s::uuid',
                            (Json(rules), rules_hash, run_id))
            master_state()
            spark = SparkSession.builder.appName(f"bronze-silver-{run_id}-{attempt}").getOrCreate()
            if spark.sparkContext.master != "spark://spark-master:7077":
                raise RuntimeError("Acceptance requires Spark standalone")
            app_id = spark.sparkContext.applicationId
            print(f"CLEAN_SPARK_START run_id={run_id} attempt_id={attempt} application_id={app_id}", flush=True)
            with conn, conn.cursor() as cur:
                cur.execute("UPDATE metadata.pipeline_runs SET spark_application_id=%s WHERE run_id=%s::uuid", (app_id, run_id))
                record_lineage(cur, run_id, attempt, 'running')
            props = {"user": os.environ["SPARK_DB_USER"], "password": os.environ["SPARK_DB_PASSWORD"],
                     "driver": "org.postgresql.Driver"}
            jdbc_url = f"jdbc:postgresql://{os.environ['LAB_DB_HOST']}:{os.environ.get('LAB_DB_PORT', '5432')}/{os.environ['LAB_DB_NAME']}"
            query = f"""(SELECT run_id::text,page_id,coalesce(final_url,requested_url) AS source_url,
                content_hash,raw_html,http_status,content_type,fetch_error FROM bronze.web_pages
                WHERE run_id='{run_id}'::uuid) AS bronze_input"""
            bronze = spark.read.jdbc(jdbc_url, query, properties=props).cache()
            input_count = bronze.count()
            if not 1 <= input_count <= 5:
                raise RuntimeError("Expected 1..5 Bronze rows")
            unique_count = bronze.select("run_id", "page_id").distinct().count()
            with conn, conn.cursor() as cur:
                record_check(cur, run_id, attempt, '@run', 'unique_keys', unique_count == input_count,
                             unique_count, input_count)
            if unique_count != input_count:
                raise RuntimeError("duplicate_key")
            transformed = classify(bronze, rules).cache()
            check_columns = [c for c in transformed.columns if c.startswith('check_')]
            observations = transformed.select('page_id', 'reject_reason', 'token_count', 'http_status',
                                               'content_type', *check_columns).collect()
            rejected = [r for r in observations if r.reject_reason]
            valid_count = input_count - len(rejected)
            quality_status = 'all_invalid' if not valid_count else 'partial' if rejected else 'valid'
            thresholds = {'required_fields': rules['checks']['required_fields'], 'fetch_success': 'no fetch_error',
                          'http_status': [rules['checks']['http_status_min'], rules['checks']['http_status_max']],
                          'content_type': rules['checks']['supported_content_types'], 'parse_body': 'body required',
                          'nonempty_clean_text': 'length > 0', 'min_token_count': rules['checks']['min_token_count'],
                          'content_hash': 'SHA256 hex'}
            with conn, conn.cursor() as cur:
                for item in observations:
                    for column in check_columns:
                        name = column.removeprefix('check_')
                        observed = {'http_status': item.http_status, 'content_type': item.content_type,
                                    'min_token_count': item.token_count}.get(name, item[column])
                        record_check(cur, run_id, attempt, item.page_id, name, item[column], observed, thresholds[name])
                    if item.reject_reason:
                        cur.execute('''INSERT INTO quarantine.rejected_pages
                            (run_id,page_id,attempt_id,reason,rules_version) VALUES (%s::uuid,%s,%s,%s,%s)''',
                            (run_id, item.page_id, attempt, item.reject_reason, rules['rules_version']))
                record_check(cur, run_id, attempt, '@run', 'input_balance', input_count == valid_count + len(rejected),
                             input_count, {'valid': valid_count, 'rejected': len(rejected)})
                record_check(cur, run_id, attempt, '@run', 'has_valid_pages', valid_count > 0, valid_count, '>0')
                cur.execute('''UPDATE metadata.pipeline_runs SET input_count=%s,valid_count=%s,rejected_count=%s,
                    quality_status=%s,total_tokens=0 WHERE run_id=%s::uuid''',
                    (input_count, valid_count, len(rejected), quality_status, run_id))
            if not valid_count:
                raise RuntimeError('all_invalid: ' + ','.join(sorted({r.reject_reason for r in rejected})))
            transformed = transformed.filter(F.col('reject_reason') == '')
            total_tokens = transformed.agg(F.sum("token_count")).first()[0]
            docs = transformed.select(
                "run_id", F.lit(attempt).alias("attempt_id"), "page_id", "source_url", "content_hash",
                F.col("parsed.title").alias("title"), F.col("parsed.clean_text").alias("clean_text"),
                "token_count", F.current_timestamp().alias("processed_at"),
                F.lit(TRANSFORM_VERSION).alias("transform_version"))
            words = transformed.select("run_id", "page_id", F.explode("tokens").alias("word"))
            counts = (words.groupBy("run_id", "page_id", "word").count()
                      .withColumn("attempt_id", F.lit(attempt))
                      .select("run_id", "attempt_id", "page_id", "word", "count"))
            docs.coalesce(1).write.jdbc(jdbc_url, "staging.documents", mode="append", properties=props)
            if args.fail_after_documents_always == 'true' or (args.fail_after_documents_once == "true" and attempt == "spark-1"):
                print(f"INJECTED_FAILURE_AFTER_DOCUMENTS run_id={run_id} application_id={app_id}", flush=True)
                raise RuntimeError("Injected acceptance failure after documents staging, before word counts")
            counts.coalesce(1).write.jdbc(jdbc_url, "staging.word_counts", mode="append", properties=props)
            try:
                with conn, conn.cursor() as cur:
                    validate_staging(cur, run_id, attempt, input_count, total_tokens, len(rejected))
            except Exception:
                with conn, conn.cursor() as cur:
                    record_check(cur, run_id, attempt, '@run', 'staging_invariants', False, 'staging mismatch',
                                 'input=valid+rejected; positive counts; per-page token sum; source/version')
                raise
            with conn, conn.cursor() as cur:
                record_check(cur, run_id, attempt, '@run', 'staging_invariants', True,
                             {'input': input_count, 'valid': valid_count, 'rejected': len(rejected), 'total_tokens': total_tokens},
                             'input=valid+rejected; positive counts; per-page token sum; source/version')
                cur.execute("""UPDATE metadata.pipeline_runs SET staging_complete=true,
                    publish_status='staged',input_count=%s,total_tokens=%s
                    WHERE run_id=%s::uuid AND active_attempt=%s""", (input_count, total_tokens, run_id, attempt))
                if cur.rowcount != 1:
                    raise RuntimeError("Stale Spark attempt cannot complete staging")
                record_lineage(cur, run_id, attempt, 'staged', {
                    'silver.documents': valid_count, 'demo.word_counts': counts.count(),
                    'quarantine.rejected_pages': len(rejected)})
            print(f"CLEAN_SPARK_SUCCESS run_id={run_id} attempt_id={attempt} application_id={app_id} "
                  f"input_count={input_count} total_tokens={total_tokens}", flush=True)
        except Exception as error:
            conn.rollback()
            # Store a safe reason, never JDBC messages/connection credentials.
            summary = str(error) if isinstance(error, RuntimeError) else type(error).__name__
            with conn, conn.cursor() as cur:
                cur.execute('SELECT quality_status FROM metadata.pipeline_runs WHERE run_id=%s::uuid', (run_id,))
                if cur.fetchone()[0] != 'all_invalid':
                    record_check(cur, run_id, attempt, '@run', 'system_failure', False,
                                 'attempt failed', 'successful execution required')
                cur.execute("""UPDATE metadata.pipeline_runs SET publish_status='failed',
                    quality_status=CASE WHEN quality_status='all_invalid' THEN quality_status ELSE 'system_failure' END,
                    staging_complete=false,error_summary=%s WHERE run_id=%s::uuid AND active_attempt=%s""",
                    (summary[:500], run_id, attempt))
                record_lineage(cur, run_id, attempt, 'failed')
            raise
        finally:
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()
