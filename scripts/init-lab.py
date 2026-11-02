"""Administrative schema/roles/fixture seed, separate from task credentials."""
import hashlib
import os
from pathlib import Path

from psycopg2 import sql
from src.common.smoke import connect, FIXTURE_PAGE, FIXTURE_RUN, FIXTURE_URL, FIXTURE_PATH
from src.common.catalog import register_catalog

with connect("LAB_ADMIN") as conn, conn.cursor() as cur:
    cur.execute(Path("/opt/lab/sql/init/001_day1.sql").read_text())
    for migration in sorted(Path("/opt/lab/sql/migrations").glob("*.sql")):
        cur.execute(migration.read_text(encoding="utf-8"))
    roles = ('CRAWLER', 'SPARK', 'PUBLISHER', 'OBSERVER')
    names = [os.environ[f'{r}_DB_USER'] for r in roles]
    if len(set(names)) != 4 or os.environ['LAB_ADMIN_DB_USER'] in names:
        raise ValueError('Four distinct task identities must differ from admin')
    for role in roles:
        username = os.environ[f"{role}_DB_USER"]
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (username,))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE").format(sql.Identifier(username)))
        cur.execute(sql.SQL('ALTER ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %s').format(sql.Identifier(username)), (os.environ[f"{role}_DB_PASSWORD"],))
        cur.execute('SELECT parent.rolname FROM pg_auth_members m JOIN pg_roles parent ON parent.oid=m.roleid JOIN pg_roles child ON child.oid=m.member WHERE child.rolname=%s', (username,))
        if cur.fetchall():
            raise RuntimeError('Task identity has unexpected role memberships; inspect before migration')
    cur.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
    cur.execute(sql.SQL('REVOKE CREATE, TEMPORARY ON DATABASE {} FROM PUBLIC').format(sql.Identifier(os.environ['LAB_DB_NAME'])))
    schemas = 'bronze, staging, silver, demo, metadata, quality, quarantine'
    for name in names:
        ident = sql.Identifier(name)
        cur.execute(sql.SQL(f'REVOKE ALL ON ALL TABLES IN SCHEMA {schemas} FROM {{}}').format(ident))
        cur.execute(sql.SQL(f'REVOKE ALL ON SCHEMA {schemas}, public FROM {{}}').format(ident))
        cur.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(sql.Identifier(os.environ['LAB_DB_NAME']), ident))
    crawler = sql.Identifier(os.environ["CRAWLER_DB_USER"])
    spark = sql.Identifier(os.environ["SPARK_DB_USER"])
    publisher = sql.Identifier(os.environ['PUBLISHER_DB_USER'])
    observer = sql.Identifier(os.environ['OBSERVER_DB_USER'])
    cur.execute(sql.SQL("GRANT USAGE ON SCHEMA bronze TO {}, {}").format(crawler, spark))
    cur.execute(sql.SQL("GRANT SELECT, INSERT ON bronze.web_pages TO {}").format(crawler))
    cur.execute(sql.SQL("GRANT SELECT ON bronze.web_pages TO {}").format(spark))
    cur.execute(sql.SQL("GRANT SELECT, INSERT ON bronze.fetch_details TO {}").format(crawler))
    cur.execute(sql.SQL("GRANT SELECT ON bronze.fetch_details TO {}").format(spark))
    cur.execute(sql.SQL("GRANT USAGE ON SCHEMA staging TO {}").format(spark))
    cur.execute(sql.SQL("GRANT SELECT, INSERT, DELETE ON staging.smoke_results TO {}").format(spark))
    cur.execute(sql.SQL("GRANT USAGE ON SCHEMA silver, demo, metadata TO {}").format(spark))
    cur.execute(sql.SQL("GRANT SELECT, INSERT, DELETE ON staging.documents, staging.word_counts TO {}").format(spark))
    cur.execute(sql.SQL('GRANT SELECT ON silver.documents, demo.word_counts TO {}').format(spark))
    cur.execute(sql.SQL("GRANT SELECT, INSERT, UPDATE ON metadata.pipeline_runs TO {}").format(spark))
    cur.execute(sql.SQL("GRANT SELECT ON demo.published_word_counts TO {}").format(spark))
    cur.execute(sql.SQL("GRANT USAGE ON SCHEMA quality, quarantine TO {}").format(spark))
    cur.execute(sql.SQL("GRANT SELECT, INSERT, UPDATE ON quality.check_results TO {}").format(spark))
    cur.execute(sql.SQL("GRANT SELECT, INSERT, DELETE ON quarantine.rejected_pages TO {}").format(spark))
    cur.execute(sql.SQL('GRANT SELECT ON metadata.datasets, metadata.catalog_versions TO {}').format(spark))
    cur.execute(sql.SQL('GRANT SELECT, INSERT, UPDATE ON metadata.lineage TO {}').format(spark))
    cur.execute(sql.SQL('GRANT USAGE ON SCHEMA bronze, staging, silver, demo, metadata, quality, quarantine TO {}').format(publisher))
    cur.execute(sql.SQL('GRANT SELECT ON bronze.web_pages, staging.documents, staging.word_counts, quarantine.rejected_pages, quality.check_results, metadata.pipeline_runs, metadata.datasets, metadata.catalog_versions, metadata.lineage TO {}').format(publisher))
    cur.execute(sql.SQL('GRANT SELECT, INSERT, DELETE ON silver.documents, demo.word_counts TO {}').format(publisher))
    cur.execute(sql.SQL('GRANT UPDATE (publish_status,published_at,error_summary) ON metadata.pipeline_runs TO {}').format(publisher))
    cur.execute(sql.SQL('GRANT UPDATE (publish_status,updated_at) ON metadata.lineage TO {}').format(publisher))
    cur.execute(sql.SQL('GRANT USAGE ON SCHEMA metadata, demo TO {}').format(observer))
    cur.execute(sql.SQL('GRANT SELECT ON metadata.datasets, metadata.lineage, metadata.run_summary, demo.published_word_counts TO {}').format(observer))
    register_catalog(cur)
    raw = FIXTURE_PATH.read_bytes()
    cur.execute("""INSERT INTO bronze.web_pages VALUES
        (%s::uuid,%s,%s,%s,now(),200,'text/html; charset=utf-8',%s,%s,'utf-8',NULL)
        ON CONFLICT (run_id,page_id) DO NOTHING""",
        (FIXTURE_RUN, FIXTURE_PAGE, FIXTURE_URL, FIXTURE_URL, hashlib.sha256(raw).hexdigest(), raw.decode("utf-8")))
print("Lab migration, non-superuser task roles and handwritten fixture seed ready.")
