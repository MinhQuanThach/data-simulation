"""Real standalone Spark transform and complete per-page word counts over JDBC."""
import argparse
import json
import os
import re
from uuid import UUID

from pyspark.sql import SparkSession, functions as F, types as T

from src.common.pipeline import check_versions, locked_run, validate_staging
from src.common.smoke import master_state
from src.common.transform import extract_html, tokenize, TRANSFORM_VERSION


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--fail-after-documents-once", choices=("true", "false"), default="false")
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
            cur.execute("""UPDATE metadata.pipeline_runs SET active_attempt=%s,
                staging_complete=false,publish_status='running',spark_application_id=NULL,
                input_count=NULL,total_tokens=NULL,error_summary=NULL WHERE run_id=%s::uuid""", (attempt, run_id))
        try:
            master_state()
            spark = SparkSession.builder.appName(f"bronze-silver-{run_id}-{attempt}").getOrCreate()
            if spark.sparkContext.master != "spark://spark-master:7077":
                raise RuntimeError("Acceptance requires Spark standalone")
            app_id = spark.sparkContext.applicationId
            print(f"CLEAN_SPARK_START run_id={run_id} attempt_id={attempt} application_id={app_id}", flush=True)
            with conn, conn.cursor() as cur:
                cur.execute("UPDATE metadata.pipeline_runs SET spark_application_id=%s WHERE run_id=%s::uuid", (app_id, run_id))
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
            if bronze.select("run_id", "page_id").distinct().count() != input_count:
                raise RuntimeError("duplicate_key")
            parsed_schema = T.StructType([T.StructField(name, T.StringType(), False)
                                          for name in ("title", "clean_text", "reason")])
            parse_udf = F.udf(extract_html, parsed_schema)
            token_udf = F.udf(tokenize, T.ArrayType(T.StringType(), False))
            transformed = (bronze.withColumn("parsed", parse_udf("raw_html"))
                           .withColumn("tokens", token_udf("parsed.clean_text"))
                           .withColumn("token_count", F.size("tokens").cast("long")))
            reason = (F.when(F.col("page_id").isNull() | F.col("source_url").isNull() |
                             (F.length(F.trim("source_url")) == 0), "missing_source")
                      .when(F.col("fetch_error").isNotNull(), "fetch_error")
                      .when(F.col("http_status").isNull() | ~F.col("http_status").between(200, 299), "http_error")
                      .when(F.col("content_type").isNull() |
                            ~F.lower(F.trim(F.split("content_type", ";").getItem(0))).isin("text/html", "application/xhtml+xml"),
                            "unsupported_content_type")
                      .when(F.col("parsed.reason") != "", F.col("parsed.reason"))
                      .when(F.col("token_count") <= 0, "no_tokens")
                      .otherwise(""))
            transformed = transformed.withColumn("reject_reason", reason).cache()
            rejected = transformed.filter(F.col("reject_reason") != "").select("page_id", "reject_reason").collect()
            if rejected:
                print("QUALITY_REJECTED " + json.dumps([r.asDict() for r in rejected]), flush=True)
                # Day-2 policy fails the fixture run. Per-page quarantine/partial
                # policy and persisted check results are implemented on day 4.
                raise RuntimeError("quality_rejected: " + ",".join(sorted({r.reject_reason for r in rejected})))
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
            if args.fail_after_documents_once == "true" and attempt == "spark-1":
                print(f"INJECTED_FAILURE_AFTER_DOCUMENTS run_id={run_id} application_id={app_id}", flush=True)
                raise RuntimeError("Injected acceptance failure after documents staging, before word counts")
            counts.coalesce(1).write.jdbc(jdbc_url, "staging.word_counts", mode="append", properties=props)
            with conn, conn.cursor() as cur:
                validate_staging(cur, run_id, attempt, input_count, total_tokens)
                cur.execute("""UPDATE metadata.pipeline_runs SET staging_complete=true,
                    publish_status='staged',input_count=%s,total_tokens=%s
                    WHERE run_id=%s::uuid AND active_attempt=%s""", (input_count, total_tokens, run_id, attempt))
                if cur.rowcount != 1:
                    raise RuntimeError("Stale Spark attempt cannot complete staging")
            print(f"CLEAN_SPARK_SUCCESS run_id={run_id} attempt_id={attempt} application_id={app_id} "
                  f"input_count={input_count} total_tokens={total_tokens}", flush=True)
        except Exception as error:
            conn.rollback()
            # Store a safe reason, never JDBC messages/connection credentials.
            summary = str(error) if isinstance(error, RuntimeError) else type(error).__name__
            with conn, conn.cursor() as cur:
                cur.execute("""UPDATE metadata.pipeline_runs SET publish_status='failed',
                    staging_complete=false,error_summary=%s WHERE run_id=%s::uuid AND active_attempt=%s""",
                    (summary[:500], run_id, attempt))
            raise
        finally:
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()
