"""Real standalone JDBC read/write. Day 1 deliberately does not clean/tokenize HTML."""
import argparse
import os
import platform
from uuid import UUID

from pyspark.sql import SparkSession, functions as F, types as T
from src.common.smoke import connect, master_state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run_id = str(UUID(args.run_id))
    master_state()
    with connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM staging.smoke_results WHERE run_id=%s", (run_id,))
    spark = SparkSession.builder.appName(f"smoke-bronze-{run_id}").getOrCreate()
    try:
        if spark.sparkContext.master != "spark://spark-master:7077":
            raise RuntimeError("Acceptance requires the standalone cluster")
        spark.sparkContext.setLogLevel("INFO")
        app_id = spark.sparkContext.applicationId
        props = {"user": os.environ["SPARK_DB_USER"], "password": os.environ["SPARK_DB_PASSWORD"],
                 "driver": "org.postgresql.Driver"}
        jdbc_url = f"jdbc:postgresql://{os.environ['LAB_DB_HOST']}:{os.environ.get('LAB_DB_PORT', '5432')}/{os.environ['LAB_DB_NAME']}"
        query = f"(SELECT run_id::text, page_id, raw_html, content_hash FROM bronze.web_pages WHERE run_id='{run_id}'::uuid) AS input"
        bronze = spark.read.jdbc(jdbc_url, query, properties=props)

        @F.udf(T.StringType())
        def worker_python(_):
            return platform.python_version()

        result = bronze.select("run_id", "page_id", "content_hash",
                               F.length(F.encode("raw_html", "UTF-8")).cast("long").alias("html_bytes"),
                               F.lit(app_id).alias("spark_application_id"),
                               worker_python("page_id").alias("executor_python"),
                               F.current_timestamp().alias("processed_at"))
        result.coalesce(1).write.jdbc(jdbc_url, "staging.smoke_results", mode="append", properties=props)
        print(f"SMOKE_SPARK_SUCCESS run_id={run_id} application_id={app_id} master={spark.sparkContext.master}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
