-- Catalog history is explicit; do not invent lineage for pre-day-5 runs.
CREATE TABLE IF NOT EXISTS metadata.catalog_versions (
    catalog_sha256 text PRIMARY KEY CHECK (catalog_sha256 ~ '^[0-9a-f]{64}$'),
    config jsonb NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS metadata.datasets (
    dataset_name text NOT NULL,
    schema_version text NOT NULL,
    owner text NOT NULL,
    description text NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_name,schema_version)
);
ALTER TABLE metadata.pipeline_runs ADD COLUMN IF NOT EXISTS catalog_sha256 text
    REFERENCES metadata.catalog_versions(catalog_sha256);
CREATE TABLE IF NOT EXISTS metadata.lineage (
    run_id uuid NOT NULL REFERENCES metadata.pipeline_runs(run_id),
    attempt_id text NOT NULL,
    input_dataset text NOT NULL,
    input_schema_version text NOT NULL,
    output_dataset text NOT NULL,
    output_schema_version text NOT NULL,
    job_name text NOT NULL,
    spark_application_id text NOT NULL,
    transform_version text NOT NULL,
    rules_version text NOT NULL,
    rules_sha256 text NOT NULL,
    catalog_sha256 text NOT NULL REFERENCES metadata.catalog_versions(catalog_sha256),
    publish_status text NOT NULL CHECK (publish_status IN ('running','staged','failed','published')),
    output_count bigint CHECK (output_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id,attempt_id,input_dataset,output_dataset),
    FOREIGN KEY (input_dataset,input_schema_version) REFERENCES metadata.datasets(dataset_name,schema_version),
    FOREIGN KEY (output_dataset,output_schema_version) REFERENCES metadata.datasets(dataset_name,schema_version)
);
CREATE OR REPLACE VIEW metadata.run_summary AS
SELECT run_id,dag_id,airflow_run_id,transform_version,rules_version,active_attempt,
    publish_status,quality_status,spark_application_id,input_count,valid_count,
    rejected_count,total_tokens,published_at,rules_sha256,catalog_sha256,error_summary
FROM metadata.pipeline_runs;

-- Job control writes cannot impersonate publication through metadata alone.
-- Invoker rights; no SECURITY DEFINER or credentials embedded in SQL.
CREATE OR REPLACE FUNCTION metadata.guard_publication() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT has_table_privilege(current_user,'silver.documents','INSERT') THEN
        IF NEW.publish_status = 'published' THEN
            RAISE EXCEPTION 'Only publisher may mark published' USING ERRCODE='42501';
        END IF;
        IF TG_TABLE_NAME = 'pipeline_runs' THEN
            IF NEW.published_at IS NOT NULL THEN
                RAISE EXCEPTION 'Only publisher may set published_at' USING ERRCODE='42501';
            END IF;
        END IF;
        IF TG_OP = 'UPDATE' AND OLD.publish_status = 'published' THEN
            RAISE EXCEPTION 'Published metadata is immutable to job' USING ERRCODE='42501';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER guard_publication BEFORE INSERT OR UPDATE ON metadata.pipeline_runs
FOR EACH ROW EXECUTE FUNCTION metadata.guard_publication();
CREATE OR REPLACE TRIGGER guard_publication BEFORE INSERT OR UPDATE ON metadata.lineage
FOR EACH ROW EXECUTE FUNCTION metadata.guard_publication();
REVOKE ALL ON FUNCTION metadata.guard_publication() FROM PUBLIC;
