-- Preserve historical snapshots and attempt diagnostics.
CREATE SCHEMA IF NOT EXISTS quality;
CREATE SCHEMA IF NOT EXISTS quarantine;
ALTER TABLE metadata.pipeline_runs ADD COLUMN IF NOT EXISTS rules_config jsonb;
ALTER TABLE metadata.pipeline_runs ADD COLUMN IF NOT EXISTS rules_sha256 text;
ALTER TABLE metadata.pipeline_runs ADD COLUMN IF NOT EXISTS valid_count bigint;
ALTER TABLE metadata.pipeline_runs ADD COLUMN IF NOT EXISTS rejected_count bigint;
ALTER TABLE metadata.pipeline_runs ADD COLUMN IF NOT EXISTS quality_status text NOT NULL DEFAULT 'pending'
    CHECK (quality_status IN ('pending','valid','partial','all_invalid','system_failure'));
CREATE TABLE IF NOT EXISTS quality.check_results (
    run_id uuid NOT NULL REFERENCES metadata.pipeline_runs(run_id),
    attempt_id text NOT NULL,
    scope_key text NOT NULL,
    check_name text NOT NULL,
    status text NOT NULL CHECK (status IN ('pass','fail')),
    observed_value text NOT NULL,
    threshold text NOT NULL,
    rules_version text NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id,attempt_id,scope_key,check_name)
);
CREATE TABLE IF NOT EXISTS quarantine.rejected_pages (
    run_id uuid NOT NULL,
    page_id text NOT NULL,
    attempt_id text NOT NULL,
    reason text NOT NULL CHECK (length(reason)>0),
    rules_version text NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id,page_id),
    FOREIGN KEY (run_id,page_id) REFERENCES bronze.web_pages(run_id,page_id)
);
