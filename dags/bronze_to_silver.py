"""Configurable web/fixture -> Bronze -> standalone Spark -> atomic publish."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from src.common.pipeline import publish_results
from src.crawler.pipeline import validate_config, crawl_to_bronze, verify_ingestion

RUN_ID = "{{ ti.xcom_pull(task_ids='crawl_to_bronze')['run_id'] }}"

with DAG(
    dag_id="bronze_to_silver", start_date=datetime(2026, 10, 29, tzinfo=timezone.utc),
    schedule=None, catchup=False, max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["day4", "quality", "web", "fixture", "silver"], render_template_as_native_obj=True,
    doc_md="conf: urls (1..5 HTTP(S)), top_n (1..100); omit urls for fixture regression. Static HTML/depth 0, robots enforced.",
) as dag:
    validate = PythonOperator(
        task_id="validate_config", python_callable=validate_config, retries=0,
        op_kwargs={"dag_id": "{{ dag.dag_id }}", "airflow_run_id": "{{ run_id }}"},
    )
    crawl = PythonOperator(
        task_id="crawl_to_bronze", python_callable=crawl_to_bronze,
        op_kwargs={"config": "{{ ti.xcom_pull(task_ids='validate_config') }}",
                   "dag_id": "{{ dag.dag_id }}", "airflow_run_id": "{{ run_id }}"},
        execution_timeout=timedelta(minutes=8),
    )
    submit = SparkSubmitOperator(
        task_id="spark_clean_validate_count", conn_id="spark_default", pool="spark_single",
        application="/opt/lab/src/jobs/clean_validate_count.py", deploy_mode="client",
        application_args=["--run-id", RUN_ID, "--attempt-id", "spark-{{ ti.try_number }}",
                          "--fail-after-documents-once", "{{ 'true' if ti.xcom_pull(task_ids='crawl_to_bronze')['fail_once'] else 'false' }}",
                          "--fail-after-documents-always", "{{ 'true' if ti.xcom_pull(task_ids='crawl_to_bronze').get('fail_always', false) else 'false' }}"],
        name="bronze-silver", executor_memory="1g", driver_memory="1g",
        executor_cores=1, total_executor_cores=1,
        conf={
            "spark.driver.host": "airflow-scheduler", "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.port": "39001", "spark.blockManager.port": "39002",
            "spark.pyspark.python": "/usr/local/bin/python",
            "spark.pyspark.driver.python": "/usr/local/bin/python",
            "spark.speculation": "false", "spark.task.maxFailures": "1",
            "spark.sql.shuffle.partitions": "1", "spark.sql.session.timeZone": "UTC",
            "spark.driver.extraClassPath": "/opt/spark/jars/postgresql-42.7.14.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/postgresql-42.7.14.jar",
        },
        execution_timeout=timedelta(minutes=5),
    )
    publish = PythonOperator(task_id="publish_results", python_callable=publish_results,
                             op_kwargs={"run_id": RUN_ID})
    verify = PythonOperator(
        task_id="verify_output", python_callable=verify_ingestion,
        op_kwargs={"run_id": RUN_ID, "mode": "{{ ti.xcom_pull(task_ids='crawl_to_bronze')['mode'] }}",
                   "top_n": "{{ ti.xcom_pull(task_ids='crawl_to_bronze')['top_n'] }}"},
    )
    validate >> crawl >> submit >> publish >> verify
