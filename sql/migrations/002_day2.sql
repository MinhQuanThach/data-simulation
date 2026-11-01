-- Additive, rerunnable migration; never truncate previous snapshots.
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS demo;
CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id uuid PRIMARY KEY,
    dag_id text NOT NULL,
    airflow_run_id text NOT NULL,
    transform_version text NOT NULL,
    rules_version text NOT NULL,
    active_attempt text,
    staging_complete boolean NOT NULL DEFAULT false,
    publish_status text NOT NULL DEFAULT 'pending'
        CHECK (publish_status IN ('pending','running','staged','failed','published')),
    spark_application_id text,
    input_count bigint,
    total_tokens bigint,
    error_summary text,
    published_at timestamptz,
    UNIQUE (dag_id, airflow_run_id)
);

-- JDBC writes UUIDs as strings. Cast only at the transaction publish boundary.
CREATE TABLE IF NOT EXISTS staging.documents (
    run_id text NOT NULL,
    attempt_id text NOT NULL,
    page_id text NOT NULL,
    source_url text NOT NULL,
    content_hash text NOT NULL,
    title text NOT NULL,
    clean_text text NOT NULL CHECK (length(btrim(clean_text)) > 0),
    token_count bigint NOT NULL CHECK (token_count > 0),
    processed_at timestamptz NOT NULL,
    transform_version text NOT NULL,
    PRIMARY KEY (run_id, attempt_id, page_id)
);
CREATE TABLE IF NOT EXISTS staging.word_counts (
    run_id text NOT NULL,
    attempt_id text NOT NULL,
    page_id text NOT NULL,
    word text NOT NULL CHECK (length(word) > 0),
    count bigint NOT NULL CHECK (count > 0),
    PRIMARY KEY (run_id, attempt_id, page_id, word),
    FOREIGN KEY (run_id, attempt_id, page_id)
        REFERENCES staging.documents (run_id, attempt_id, page_id)
);
CREATE TABLE IF NOT EXISTS silver.documents (
    run_id uuid NOT NULL,
    page_id text NOT NULL,
    source_url text NOT NULL,
    content_hash text NOT NULL,
    title text NOT NULL,
    clean_text text NOT NULL CHECK (length(btrim(clean_text)) > 0),
    token_count bigint NOT NULL CHECK (token_count > 0),
    processed_at timestamptz NOT NULL,
    transform_version text NOT NULL,
    PRIMARY KEY (run_id, page_id),
    FOREIGN KEY (run_id, page_id) REFERENCES bronze.web_pages (run_id, page_id)
);
CREATE TABLE IF NOT EXISTS demo.word_counts (
    run_id uuid NOT NULL,
    page_id text NOT NULL,
    word text NOT NULL CHECK (length(word) > 0),
    count bigint NOT NULL CHECK (count > 0),
    PRIMARY KEY (run_id, page_id, word),
    FOREIGN KEY (run_id, page_id) REFERENCES silver.documents (run_id, page_id)
);
CREATE OR REPLACE VIEW demo.published_word_counts AS
SELECT w.* FROM demo.word_counts w
JOIN metadata.pipeline_runs r USING (run_id)
WHERE r.publish_status = 'published';
