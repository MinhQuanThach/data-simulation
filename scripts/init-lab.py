"""Administrative schema/roles/fixture seed, separate from task credentials."""
import hashlib
import os
from pathlib import Path

from psycopg2 import sql
from src.common.smoke import connect, FIXTURE_PAGE, FIXTURE_RUN, FIXTURE_URL, FIXTURE_PATH

with connect("LAB_ADMIN") as conn, conn.cursor() as cur:
    cur.execute(Path("/opt/lab/sql/init/001_day1.sql").read_text())
    for role in ("CRAWLER", "SPARK"):
        username = os.environ[f"{role}_DB_USER"]
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (username,))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE").format(sql.Identifier(username)))
        cur.execute(sql.SQL("ALTER ROLE {} PASSWORD %s").format(sql.Identifier(username)), (os.environ[f"{role}_DB_PASSWORD"],))
    cur.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
    crawler = sql.Identifier(os.environ["CRAWLER_DB_USER"])
    spark = sql.Identifier(os.environ["SPARK_DB_USER"])
    cur.execute(sql.SQL("GRANT USAGE ON SCHEMA bronze TO {}, {}").format(crawler, spark))
    cur.execute(sql.SQL("GRANT SELECT, INSERT ON bronze.web_pages TO {}").format(crawler))
    cur.execute(sql.SQL("GRANT SELECT ON bronze.web_pages TO {}").format(spark))
    cur.execute(sql.SQL("GRANT USAGE ON SCHEMA staging TO {}").format(spark))
    cur.execute(sql.SQL("GRANT SELECT, INSERT, DELETE ON staging.smoke_results TO {}").format(spark))
    raw = FIXTURE_PATH.read_bytes()
    cur.execute("""INSERT INTO bronze.web_pages VALUES
        (%s::uuid,%s,%s,%s,now(),200,'text/html; charset=utf-8',%s,%s,'utf-8',NULL)
        ON CONFLICT (run_id,page_id) DO NOTHING""",
        (FIXTURE_RUN, FIXTURE_PAGE, FIXTURE_URL, FIXTURE_URL, hashlib.sha256(raw).hexdigest(), raw.decode("utf-8")))
print("Lab migration, non-superuser task roles and handwritten fixture seed ready.")
