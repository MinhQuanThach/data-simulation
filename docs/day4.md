# Ngày 4: Data Quality và quarantine

Tài liệu này mô tả mốc ngày 4. Từ ngày 5 publisher dùng identity riêng và có
catalog/lineage; quyền và vận hành hiện tại tại [day5.md](day5.md), kết quả tại STATUS.

Web/fixture dùng chung DAG năm task và Spark standalone. Rules `1.1.0`, transform
`1.0.0`; không thêm service/dependency hoặc build lại image. Kết quả chạy thật tại
[STATUS](../STATUS.md). Quyền publisher/observer, catalog/lineage đầy đủ thuộc ngày 5;
telemetry/Grafana thuộc ngày 6. Spark/publisher tạm chung `lab_spark`.

## Vận hành

PowerShell tại `D:\VCB\data-simulation`, runtime ngày 1 đã có:

```powershell
docker compose up -d --no-build --wait --wait-timeout 360
python scripts/day4.py migrate
python scripts/day4.py run --case mixed
python scripts/day4.py accept
$day4 = Get-Content docs/evidence/day4/latest.json -Raw | ConvertFrom-Json
python scripts/day4.py inspect --run-id $day4.partial_retry_replay_run
docker compose cp sql/queries/day4_quality.sql lab-db:/tmp/day4_quality.sql
docker compose exec -T lab-db psql -X -v ON_ERROR_STOP=1 -U lab_admin -d lab -v "run_id=$($day4.partial_retry_replay_run)" -f /tmp/day4_quality.sql
```

Máy chưa có runtime: `python scripts/init-env.py`, rồi `python scripts/day1.py up`.
Migration additive/rerunnable, giữ lịch sử. Dừng giữ dữ liệu:
`python scripts/day1.py stop`. Không đổi `.env` hoặc reset volumes.

UI Airflow vẫn nhận `urls`/`top_n`. Bỏ `urls` để dùng fixture. Các fixture_case:
`valid_vi`, `mixed`, `all_invalid`, `empty`, `script_only`, `http_error`,
`missing_body`, `no_tokens`, `unsupported_type`. `mixed` có 5 trang viết tay:
valid/empty/script-only/HTTP404/missing-body; `all_invalid` thay valid bằng dấu câu.
HTTP404/type fixture giữ body valid để chứng minh HTTP gate vẫn loại trang đó.
Fixture không được trình bày như crawl web thật.

`test_fail_after_documents_once` và `test_fail_after_documents_always` là fault
injection cho lab: fail giữa hai lần ghi JDBC, tương ứng một hoặc mọi attempt.
Mặc định false; không bật đồng thời.

## Rules và quyết định publish

`src/common/quality.py` đọc YAML tại driver; Spark UDF/DataFrame expressions parse,
tokenize và kiểm tra trên executor. Driver chỉ thu kết quả check nhỏ (≤5 trang)
để persist quality/quarantine; counts do Spark explode/groupBy/count thực hiện.

Reader xác nhận schema/version/policy hỗ trợ. HTTP range chỉ được thu hẹp trong
2xx, types trong HTML/XHTML, min_token_count integer ≥1. Parser/body/NFC/
tokenization và mandatory checks là hợp đồng cố định; giá trị chưa hỗ trợ/key lạ
làm fail. Thêm algorithm/policy mới cần đổi code/version. Run lưu rules JSONB và
SHA256 canonical JSON. Retry/publish khi hash thay đổi bị chặn, cần DagRun mới.
Published snapshot replay no-op; lịch sử rules1.0.0 vẫn đọc được.

| Trường hợp | Kết quả |
|---|---|
| Mọi trang hợp lệ | quality_status=valid, publish_status=published, DAG success |
| Có trang tốt và lỗi nguồn | Quarantine lỗi; publish tốt; quality_status=partial, publish_status=published, DAG success |
| Không còn trang hợp lệ | Giữ quality/quarantine, quality_status=all_invalid, publish_status=failed; Spark/DAG failed, downstream upstream_failed |
| DB/JDBC/runtime/config lỗi, duplicate key hoặc invariant sai | quality_status=system_failure, publish_status=failed, staging_complete=false, không công bố |

Partial nằm ở `quality_status` riêng để giữ ý nghĩa transaction của `publish_status`.
Demo view lọc published nên đọc cả valid và partial đã commit. Lỗi kết nối lấy
một URL/HTTP404 có thể cách ly; lỗi DB/Spark là lỗi hệ thống, không biến thành trang lỗi.

Check trang: required page/source, fetch_error, HTTP status/type, parse/body,
text không rỗng, token minimum, hash SHA256 đúng định dạng. Reason ưu tiên theo
thứ tự này; quality vẫn ghi mọi check nên một trang có thể có nhiều check fail.
Parser sửa được một số markup lỗi; đây không phải validator XHTML. Reason:
empty_clean_text, missing_body, no_tokens, below_min_token_count, unsupported_content_type,
missing_source, missing_content_hash hoặc exact crawler error như http_404/robots_denied.

URL trùng sau chuẩn hóa được dedup trước crawl. PK Bronze chặn insert trùng;
duplicate key còn sót trong Spark làm fail toàn run, persist unique_keys=fail,
không tùy tiện chọn một bản ghi để publish/quarantine.

## Schema và retry

Migration `004_day4.sql` tạo:

- quality.check_results: PK(run_id,attempt_id,scope_key,check_name); scope_key là
  page SHA256 hoặc @run, không nullable. Status pass/fail; observed_value/threshold
  là JSON lưu dưới dạng text; rules_version, checked_at. Giữ history từng attempt.
- quarantine.rejected_pages: PK(run_id,page_id), FK Bronze; attempt_id, reason,
  rules_version, rejected_at. Không chép HTML; giữ kết quả attempt hiện tại.
- metadata.pipeline_runs: rules_config/rules_sha256, valid_count/rejected_count,
  quality_status. Không sinh quality giả cho lịch sử ngày 1–3.

Attempt mới dọn staging/quarantine chỉ của run chưa publish. Quality/quarantine/
thống kê phân loại commit chung trước khi quyết định all-invalid. Chỉ valid vào
staging. Chỉ mark complete sau khi SQL kiểm input=valid+rejected, coverage Bronze,
không overlap, đúng attempt, counts dương (DB constraints), per-page/run token sum
và source/hash/transform khớp Bronze.

Publisher kiểm lại run quality checks và SQL invariants, commit Silver/demo/publish
flag trong một transaction. Lỗi giữa chừng rollback cả hai bảng. Published replay
giữ timestamp/app ID/checks/quarantine. Xem quyết định hiện tại bằng cách lọc
quality.attempt_id=metadata.active_attempt; lịch sử có cả attempts đã fail.
Nếu Python publisher gặp lỗi, metadata giữ trạng thái staged sau rollback để task
retry kiểm lại và publish an toàn; DAG vẫn fail nếu hết retry. system_failure trong
bảng policy là trạng thái do Spark ghi, không thay cho task state của Airflow.

## Nghiệm thu và xử lý lỗi

`accept` chạy unit ngày2/3/4, oracle, partial với Airflow retry thật, exact-run
clear/replay, empty/script-only/HTTP404/all-invalid/type, system failure cả hai
attempts, DB duplicate PK, rollback/visibility, counts bị sửa sai và rules drift.
Test Spark riêng dùng standalone thật cho token threshold/missing source và inject
duplicate ở biên JDBC read vì DB có PK; không sửa Bronze để giả dữ liệu nguồn.
Cuối bộ test crawl thật example.com+URL404+URL lặp, kiểm partial/dedup.
URL lấy từ config/day3_acceptance.json. Nguồn có thể đổi; không tắt robots/TLS để PASS.

Evidence ở docs/evidence/day4/: unit transcripts; từng run có report/SQL output/task
và executor logs; retry-incomplete/replay/publish-blocked; DB/transaction/Spark
policy tests; quality-psql.txt. latest.json chỉ ghi sau toàn bộ PASS và source hashes
không đổi trong nghiệm thu. Evidence local Git ignore; runner/tests tái tạo được.
Các DagRun failed có chủ đích vẫn hiện đỏ trên UI.

Nếu lỗi: xem initial/task-logs.txt, initial/sql-output.json, `docker compose ps -a`,
`docker compose exec -T airflow-scheduler airflow dags list-import-errors`.
Khi accept PASS không cần bước sửa tay. Hạn chế static HTML/batch/tokenization
giản lược vẫn như đặc tả.

Nguồn đối chiếu: [Spark JDBC 4.0.2](https://spark.apache.org/docs/4.0.2/sql-data-sources-jdbc.html),
[PostgreSQL 16 transactions](https://www.postgresql.org/docs/16/tutorial-transactions.html).
