-- Run with observer credentials (scripts/day5.py inspect --run-id <UUID>).
SELECT r.run_id,r.dag_id,r.airflow_run_id,l.attempt_id,l.input_dataset,
    l.input_schema_version,i.owner AS input_owner,l.job_name,l.spark_application_id,
    l.output_dataset,l.output_schema_version,o.owner AS output_owner,l.output_count,
    l.transform_version,l.rules_version,l.rules_sha256,l.catalog_sha256,
    l.publish_status,r.quality_status
FROM metadata.run_summary r JOIN metadata.lineage l USING (run_id)
JOIN metadata.datasets i ON (i.dataset_name,i.schema_version)=(l.input_dataset,l.input_schema_version)
JOIN metadata.datasets o ON (o.dataset_name,o.schema_version)=(l.output_dataset,l.output_schema_version)
WHERE r.run_id=:'run_id'::uuid ORDER BY l.attempt_id,l.output_dataset;
