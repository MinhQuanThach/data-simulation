"""Selected runtime facts without environment variables or credentials."""
import hashlib
from importlib.metadata import distribution, version
import json
from pathlib import Path
import platform
import subprocess


def main():
    check = subprocess.run(["python", "-m", "pip", "check"], capture_output=True, text=True)
    if check.returncode:
        raise RuntimeError(check.stdout + check.stderr)
    java = subprocess.run(["java", "-version"], capture_output=True, text=True, check=True)
    spark = subprocess.run(["spark-submit", "--version"], capture_output=True, text=True, check=True)
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    assert SparkSubmitOperator is not None
    jar = Path("/opt/spark/jars/postgresql-42.7.14.jar")
    result = {"python": platform.python_version(), "java": java.stderr.strip(),
              "spark_submit": (spark.stdout + spark.stderr).strip(), "pip_check": check.stdout.strip(),
              "packages": {name: version(name) for name in ("apache-airflow", "apache-airflow-providers-apache-spark", "pyspark", "py4j", "psycopg2-binary")},
              "jdbc_sha256": hashlib.sha256(jar.read_bytes()).hexdigest()}
    result["pyspark_source"] = json.loads(distribution("pyspark").read_text("direct_url.json"))
    assert result["python"].startswith("3.11.")
    assert result["packages"]["pyspark"] == "4.0.2"
    assert "version 4.0.2" in result["spark_submit"]
    assert result["jdbc_sha256"] == "73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c"
    assert result["pyspark_source"]["archive_info"]["hashes"]["sha256"] == "938b4a1883383374d331ebfcb5d92debfa1891cf3d7a6d730520a1a2d23f1a90"
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
