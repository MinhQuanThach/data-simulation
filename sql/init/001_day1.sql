-- Executed transactionally by scripts/init-lab.py, not on every DAG run.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS bronze.web_pages (
    run_id uuid NOT NULL,
    page_id text NOT NULL,
    requested_url text NOT NULL,
    final_url text,
    fetched_at timestamptz NOT NULL,
    http_status integer,
    content_type text,
    content_hash text,
    raw_html text,
    response_encoding text,
    fetch_error text,
    PRIMARY KEY (run_id, page_id)
);
-- JDBC-friendly text UUID for the disposable day-1 diagnostic output.
-- Day-2 business staging tables will have their own migration.
CREATE TABLE IF NOT EXISTS staging.smoke_results (
    run_id text NOT NULL,
    page_id text NOT NULL,
    html_bytes bigint NOT NULL CHECK (html_bytes > 0),
    content_hash text NOT NULL,
    spark_application_id text NOT NULL,
    executor_python text NOT NULL,
    processed_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, page_id)
);
