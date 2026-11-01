-- Preserve the original Bronze column order for day-1/2 fixture writers.
-- Bytes are kept separately so decoding is reversible even for rejected input.
CREATE TABLE IF NOT EXISTS bronze.fetch_details (
    run_id uuid NOT NULL,
    page_id text NOT NULL,
    raw_body bytea,
    body_bytes bigint NOT NULL CHECK (body_bytes >= 0),
    request_history jsonb NOT NULL,
    robots_history jsonb NOT NULL,
    response_headers jsonb NOT NULL,
    crawler_version text NOT NULL,
    PRIMARY KEY (run_id,page_id),
    FOREIGN KEY (run_id,page_id) REFERENCES bronze.web_pages (run_id,page_id)
);
ALTER TABLE metadata.pipeline_runs ADD COLUMN IF NOT EXISTS input_config jsonb;
ALTER TABLE metadata.pipeline_runs ADD COLUMN IF NOT EXISTS requested_count integer;
ALTER TABLE metadata.pipeline_runs ADD COLUMN IF NOT EXISTS unique_input_count integer;
ALTER TABLE metadata.pipeline_runs ADD COLUMN IF NOT EXISTS crawl_complete boolean NOT NULL DEFAULT false;
