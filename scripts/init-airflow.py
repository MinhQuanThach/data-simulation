"""Initialize Airflow and a local SimpleAuthManager password file without logging it."""
import json
import os
from pathlib import Path
import subprocess

password_file = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
password_file.write_text(json.dumps({os.environ["AIRFLOW_ADMIN_USER"]: os.environ["AIRFLOW_ADMIN_PASSWORD"]}))
password_file.chmod(0o600)
subprocess.run(["airflow", "db", "migrate"], check=True)
subprocess.run(["airflow", "pools", "set", "spark_single", "1", "Single standalone Spark driver"], check=True)
print("Airflow migrated; local UI password stored in ignored .env and auth volume.")
