"""Day-2 fixture -> standalone Spark -> atomic Silver/demo publish."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from src.common.pipeline import prepare_fixture, publish_results, verify_output

RUN_ID = "{{ ti.xcom_pull(task_ids='prepare_fixture')['run_id'] }}"

with DAG(
    dag_id="bronze_to_silver", start_date=datetime(2026, 10, 29, tzinfo=timezone.utc),
    schedule=None, catchup=False, max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["day2", "fixture", "silver"],
    doc_md="Handwritten fixtures only. conf: fixture_case, top_n; test_fail_after_documents_once for retry acceptance.",
) as dag:
    prepare = PythonOperator(
        task_id="prepare_fixture", python_callable=prepare_fixture,
        op_kwargs={"dag_id": "{{ dag.dag_id }}", "airflow_run_id": "{{ run_id }}"},
    )
    submit = SparkSubmitOperator(
        task_id="spark_clean_validate_count", conn_id="spark_default", pool="spark_single",
        application="/opt/lab/src/jobs/clean_validate_count.py", deploy_mode="client",
        application_args=["--run-id", RUN_ID, "--attempt-id", "spark-{{ ti.try_number }}",
                          "--fail-after-documents-once", "{{ ti.xcom_pull(task_ids='prepare_fixture')['fail_once'] | lower }}"],
        name="day2-bronze-silver", executor_memory="1g", driver_memory="1g",
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
        task_id="verify_output", python_callable=verify_output,
        op_kwargs={"run_id": RUN_ID, "top_n": "{{ ti.xcom_pull(task_ids='prepare_fixture')['top_n'] }}"},
    )
    prepare >> submit >> publish >> verify
