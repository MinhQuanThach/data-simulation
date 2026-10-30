-- Run with psql as lab admin (host operator only).
SELECT run_id, page_id, html_bytes, content_hash,
       spark_application_id, executor_python, processed_at
FROM staging.smoke_results ORDER BY processed_at DESC;

SELECT rolname, rolsuper, rolcreatedb, rolcreaterole
FROM pg_roles WHERE rolname IN ('lab_crawler','lab_spark');
