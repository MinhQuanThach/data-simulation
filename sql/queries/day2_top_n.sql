-- psql -v run_id=<UUID> -v top_n=3 -f /tmp/day2_top_n.sql
-- Rank per page; retain every count in demo.word_counts.
SELECT page_id,word,count FROM (
    SELECT page_id,word,count,
           row_number() OVER (PARTITION BY page_id ORDER BY count DESC,word COLLATE "C" ASC) AS rank
    FROM demo.published_word_counts WHERE run_id=:'run_id'::uuid
) ranked WHERE rank <= :top_n ORDER BY page_id,rank;

SELECT d.page_id,d.title,d.clean_text,d.token_count,sum(w.count) AS counted_tokens
FROM silver.documents d JOIN demo.published_word_counts w USING (run_id,page_id)
WHERE d.run_id=:'run_id'::uuid
GROUP BY d.page_id,d.title,d.clean_text,d.token_count ORDER BY d.page_id;
