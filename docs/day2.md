# Vận hành ngày 2: fixture → Silver

Luồng ngày 2 ban đầu: `prepare_fixture → spark_clean_validate_count → publish_results
→ verify_output` trong DAG `bronze_to_silver`. Từ ngày 3, hai task
`validate_config → crawl_to_bronze` thay `prepare_fixture`, tổng cộng năm task;
không có `urls` thì vẫn chọn fixture. Runner ngày 2 đã cập nhật kiểm năm task.
DAG ngày 1 `smoke_bronze` vẫn giữ
để chẩn đoán JDBC. Đầu vào là fixture viết tay; không truy cập domain `.invalid`.
Kết quả nghiệm thu và run IDs xem [STATUS](../STATUS.md).

## Khởi động và chạy

Tại PowerShell ở `D:\VCB\data-simulation`, dùng image đã build ngày 1:

```powershell
python scripts/init-env.py
docker compose up -d --no-build --wait --wait-timeout 360
python scripts/day2.py migrate
python scripts/day2.py run
```

Không cần build lại image hoặc cài thêm dependency cho ngày 2. Nếu chưa có
runtime local, chạy `python scripts/day1.py up` trước. `migrate` gọi `lab-init`
bằng admin riêng, áp dụng SQL additive/rerunnable, cấp quyền và giữ lịch sử cũ.
Không xóa volumes hay sinh lại `.env` đã có.

`run` trigger DagRun mới bằng CLI, đợi Airflow scheduler chạy đủ năm task hiện tại,
kiểm tra oracle và executor logs rồi lưu evidence. Spark submit dùng standalone
`spark://spark-master:7077`, client mode; driver trong scheduler, executor trên worker.

Chạy bộ nghiệm thu đầy đủ:

```powershell
python scripts/day2.py accept
```

Bộ này chạy 5 unit tests trong runtime Linux; tạo DagRun fixture bình thường;
tạo DagRun cố ý lỗi sau khi ghi staging documents và trước khi ghi counts để
Airflow retry task thật; clear **đúng logical date của DagRun thử nghiệm đó** để
chạy lại năm task trên snapshot đã publish; thử rollback/visibility bằng connection
PostgreSQL riêng; cuối cùng chạy fixture empty và script-only, kỳ vọng DAG failed.
Các DagRun failed có chủ đích được giữ trên UI; đây là kết quả đúng của test lỗi.
Các snapshot thử nghiệm cũng được giữ để đối chiếu, không phải dữ liệu web thật.

Evidence gồm `report.json`, `sql-output.json`, task logs, executor logs cho từng
application, `retry-incomplete.json` và `transaction-tests.txt`. Secrets được lọc
trước khi lưu; `docs/evidence/` bị Git ignore. `docs/evidence/day2/latest.json`
chỉ cập nhật khi toàn bộ bộ nghiệm thu PASS, không được suy ra lần chạy sau đó
cũng PASS nếu chưa đọc timestamp/report. Nghiệm thu tuần/cold start thuộc ngày 7.

## Xem kết quả

```powershell
$day2 = Get-Content docs/evidence/day2/latest.json -Raw | ConvertFrom-Json
python scripts/day2.py inspect --run-id $day2.normal_run
docker compose cp sql/queries/day2_top_n.sql lab-db:/tmp/day2_top_n.sql
docker compose exec -T lab-db psql -X -v ON_ERROR_STOP=1 -U lab_admin -d lab -v "run_id=$($day2.normal_run)" -v top_n=3 -f /tmp/day2_top_n.sql
```

`inspect` trả metadata, Silver, **toàn bộ** counts và top-N. SQL dùng
`row_number()` theo từng page, sắp `count DESC, word COLLATE "C" ASC`. `top_n`
chỉ giới hạn hiển thị; bảng counts giữ đủ 8 từ, tổng count = 12. Oracle tiếng Việt:
`Xin chào, Việt Nam! Xin chào & dữ liệu 2026. Dữ liệu sạch.`; top-3 `chào`, `dữ`,
`liệu`, mỗi từ 2 lần. Title không thuộc clean_text.

Có thể trigger bằng UI Airflow với config:

```json
{"fixture_case":"valid_vi","top_n":3}
```

`fixture_case` hỗ trợ `valid_vi`, `empty`, `script_only`; `top_n` là số nguyên 1–100.
Flag `test_fail_after_documents_once: true` chỉ phục vụ nghiệm thu retry: Spark
attempt `spark-1` sẽ fail; attempt sau dọn staging của run và chạy lại. Không bật
flag này cho luồng bình thường. XCom chỉ giữ UUID/cấu hình/thống kê nhỏ.

## Schema và retry/publish

- `metadata.pipeline_runs`: UUID run, Airflow identifiers, versions, active attempt,
  staging_complete, publish_status, application ID, input_count, total_tokens,
  error_summary, published_at. Đây là control metadata tối thiểu; chưa có catalog/lineage.
- `staging.documents`: PK `(run_id text,attempt_id,page_id)`; `staging.word_counts`:
  PK `(run_id text,attempt_id,page_id,word)`, FK về staging documents. Text UUID giúp
  JDBC ghi string; publisher cast sang UUID tại biên publish.
- `silver.documents`: PK `(run_id uuid,page_id)`, FK về Bronze; `demo.word_counts`:
  PK `(run_id uuid,page_id,word)`, FK về Silver; counts/token_count phải dương.
- `demo.published_word_counts`: view chỉ đọc run có publish_status=`published`.

Run UUIDv5 giữ hợp đồng compact JSON `[dag_id, Airflow run_id]`; DagRun mới là
snapshot mới. `attempt_id=spark-<Airflow try_number>`. Trước mỗi Spark attempt
chưa publish, xóa counts rồi documents **chỉ trong run đó**, reset completion;
ghi JDBC append có PK/FK, kiểm counts/source/version rồi mới đánh dấu `staged`.

Spark và publisher dùng cùng PostgreSQL advisory lock theo run, giữ xuyên các
commit của attempt; đóng session tự nhả lock. Publisher khóa thêm metadata row,
chặn incomplete/failed staging, kiểm số trang/tổng token/source/version, rồi dùng
một transaction thay Silver, thay counts và cập nhật published flag/time. Lỗi
ở giữa rollback cả hai bảng và metadata. View demo chỉ đọc dữ liệu đã commit/publish.
Run đã publish cùng versions chạy lại transform/publish là no-op; timestamps và
application ID gốc giữ nguyên. Không recrawl/retransform snapshot đã công bố.

## Quy tắc và giới hạn hiện tại

Transform/rules `1.0.0`: BeautifulSoup `html.parser`, lấy toàn bộ body text, bỏ
script/style/noscript/template, decode entities, NFC và collapse whitespace.
Token lowercase rồi NFC, giữ chuỗi chữ Unicode category L và số Nd; dấu câu,
underscore là separator. Không stopword/stemming; tiếng Việt chưa phân đoạn từ.
Parser/tokenization chạy trong Spark UDF trên executor; counts dùng Spark
`explode/groupBy/count`. Không cam kết loại mọi navigation/boilerplate.

Ngày 2 chặn missing body, empty text, zero tokens và đầu vào HTTP/type không hợp lệ;
gặp trang lỗi thì fail toàn run trước publish. Persisted quality/quarantine và
policy publish partial sẽ làm ở ngày 4; YAML hiện là hợp đồng, chưa được job đọc.
`error_summary` và log là bằng chứng lỗi tối thiểu, không phải bảng quality đầy đủ.

Task không dùng admin: fixture writer dùng `lab_crawler`; Spark và Python publisher
tạm dùng chung `lab_spark`. Tách publisher identity/ma trận allow–deny thuộc ngày 5.
Metadata catalog/lineage ngày 5; telemetry/Grafana ngày 6. Crawler web ngày 3 và
snapshot Bronze có bytes/HTTP audit được hướng dẫn riêng ở [day3.md](day3.md).
Giữ pins/runtime/Compose ngày 1; không thêm service hoặc tải JAR lúc task chạy.

Dừng và giữ dữ liệu: `python scripts/day1.py stop`. Khi lỗi, xem report/task logs,
`docker compose ps -a` và `docker compose exec -T airflow-scheduler airflow dags
list-import-errors`. Không dùng prune/down -v để xử lý lỗi.

Nguồn chính thức đã đối chiếu khi triển khai:
[Spark JDBC 4.0.2](https://spark.apache.org/docs/4.0.2/sql-data-sources-jdbc.html),
[PostgreSQL 16 locks](https://www.postgresql.org/docs/16/explicit-locking.html),
[PostgreSQL transactions](https://www.postgresql.org/docs/16/tutorial-transactions.html).
