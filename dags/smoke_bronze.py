"""Day-1 acceptance: Airflow -> standalone executor -> PostgreSQL JDBC."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from src.common.smoke import prepare_run, verify_run

with DAG(
    dag_id="smoke_bronze", start_date=datetime(2026, 10, 29, tzinfo=timezone.utc),
    schedule=None, catchup=False, max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["day1", "fixture", "jdbc"],
) as dag:
    prepare = PythonOperator(
        task_id="prepare_fixture", python_callable=prepare_run,
        op_kwargs={"dag_id": "{{ dag.dag_id }}", "airflow_run_id": "{{ run_id }}"},
    )
    submit = SparkSubmitOperator(
        task_id="spark_jdbc_smoke", conn_id="spark_default", pool="spark_single",
        application="/opt/lab/src/jobs/smoke_jdbc.py", deploy_mode="client",
        application_args=["--run-id", "{{ ti.xcom_pull(task_ids='prepare_fixture') }}"],
        name="day1-bronze-jdbc", executor_memory="1g", driver_memory="1g",
        executor_cores=1, total_executor_cores=1,
        conf={
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
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
    verify = PythonOperator(
        task_id="verify_staging", python_callable=verify_run,
        op_kwargs={"run_id": "{{ ti.xcom_pull(task_ids='prepare_fixture') }}"},
    )
    prepare >> submit >> verify
