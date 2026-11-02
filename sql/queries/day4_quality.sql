-- psql -v run_id=<UUID> -f /tmp/day4_quality.sql
SELECT run_id,publish_status,quality_status,input_count,valid_count,rejected_count,
       total_tokens,active_attempt,rules_version,rules_sha256,error_summary
FROM metadata.pipeline_runs WHERE run_id=:'run_id'::uuid;
SELECT q.page_id,q.reason,q.attempt_id,b.requested_url,b.http_status
FROM quarantine.rejected_pages q JOIN bronze.web_pages b USING(run_id,page_id)
WHERE q.run_id=:'run_id'::uuid ORDER BY q.page_id;
SELECT c.attempt_id,c.scope_key,c.check_name,c.status,c.observed_value,c.threshold
FROM quality.check_results c JOIN metadata.pipeline_runs r USING(run_id)
WHERE c.run_id=:'run_id'::uuid AND c.attempt_id=r.active_attempt
ORDER BY c.scope_key,c.check_name;
SELECT d.page_id,d.token_count,sum(w.count) AS counted_tokens
FROM silver.documents d JOIN demo.published_word_counts w USING(run_id,page_id)
WHERE d.run_id=:'run_id'::uuid GROUP BY d.page_id,d.token_count;
