"""Administrative schema/roles/fixture seed, separate from task credentials."""
import hashlib
import os
from pathlib import Path

from psycopg2 import sql
from src.common.smoke import connect, FIXTURE_PAGE, FIXTURE_RUN, FIXTURE_URL, FIXTURE_PATH

with connect("LAB_ADMIN") as conn, conn.cursor() as cur:
    cur.execute(Path("/opt/lab/sql/init/001_day1.sql").read_text())
    for migration in sorted(Path("/opt/lab/sql/migrations").glob("*.sql")):
        cur.execute(migration.read_text(encoding="utf-8"))
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
    # Day 2 shares the job identity with the Python publisher. Split publisher
    # credentials and test the full allow/deny matrix on day 5.
    cur.execute(sql.SQL("GRANT USAGE ON SCHEMA silver, demo, metadata TO {}").format(spark))
    cur.execute(sql.SQL("GRANT SELECT, INSERT, DELETE ON staging.documents, staging.word_counts, silver.documents, demo.word_counts TO {}").format(spark))
    cur.execute(sql.SQL("GRANT SELECT, INSERT, UPDATE ON metadata.pipeline_runs TO {}").format(spark))
    cur.execute(sql.SQL("GRANT SELECT ON demo.published_word_counts TO {}").format(spark))
    raw = FIXTURE_PATH.read_bytes()
    cur.execute("""INSERT INTO bronze.web_pages VALUES
        (%s::uuid,%s,%s,%s,now(),200,'text/html; charset=utf-8',%s,%s,'utf-8',NULL)
        ON CONFLICT (run_id,page_id) DO NOTHING""",
        (FIXTURE_RUN, FIXTURE_PAGE, FIXTURE_URL, FIXTURE_URL, hashlib.sha256(raw).hexdigest(), raw.decode("utf-8")))
print("Lab migration, non-superuser task roles and handwritten fixture seed ready.")
