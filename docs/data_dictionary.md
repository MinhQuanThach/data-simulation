# Hợp đồng dữ liệu

Hiện tại ngày 4: quality.check_results và quarantine.rejected_pages đã được tạo
bằng migration004; metadata có rules_config/rules_sha256, quality_status,
valid_count/rejected_count. `partial` là quality_status, còn publish_status của
mọi snapshot đã commit vẫn là `published`. Chi tiết schema tại [day4.md](day4.md).
Các đoạn “dự kiến” về catalog/lineage/telemetry dưới đây vẫn chờ ngày 5–6.

Thiết kế nghiệp vụ version 1.0.0; ngày 2 đã thêm staging/Silver/demo và control metadata.
Timestamp dùng PostgreSQL timestamptz, lưu UTC; UI/tài liệu người dùng theo Asia/Saigon.

Cập nhật ngày 1: `sql/init/001_day1.sql` đã hiện thực `bronze.web_pages` và bảng
chẩn đoán `staging.smoke_results` (run_id text, page_id, html_bytes, content_hash,
spark_application_id, executor_python, processed_at; PK run_id/page_id).
Bronze giữ UUID và đủ HTTP/source fields. Bảng smoke không phải Silver staging,
chỉ chứng minh đọc/ghi JDBC và không tính word-count.

Cập nhật ngày 2: `sql/migrations/002_day2.sql` hiện thực staging.documents,
staging.word_counts, silver.documents, demo.word_counts, demo.published_word_counts
và metadata.pipeline_runs tối thiểu. Staging dùng run_id text để JDBC ghi UUID
dưới dạng string; publisher cast sang UUID ở các bảng công bố. Metadata lưu
active_attempt, staging_complete, application ID, versions, trạng thái và metrics
tối thiểu. Các bảng quarantine/quality/catalog/lineage/telemetry còn là kế hoạch.
Chi tiết implementation và chính sách ngày 2 tại [day2.md](day2.md).

Cập nhật ngày 3: [003_day3.sql](../sql/migrations/003_day3.sql) thêm
`bronze.fetch_details` PK/FK `(run_id,page_id)` giữ `raw_body bytea` sau giải nén,
`body_bytes`, `request_history`, `robots_history`, response headers chọn lọc và
`crawler_version=1.0.0`. `web_pages` giữ nguyên các cột cũ; `raw_html` là text đã
decode theo HTTP charset hoặc BOM/meta/detect. Không lưu/hash body prefix khi đọc
dở/vượt giới hạn. Metadata thêm `input_config`, `requested_count`,
`unique_input_count`, `crawl_complete`; `input_config` khóa cấu hình của snapshot.
Fixture ngày 1–2 không có fetch_details; web snapshot luôn có cả hai bảng Bronze.
Replay dùng snapshot đã commit, không HTTP mới. Chi tiết: [day3.md](day3.md).

## Định danh và snapshot

- `run_id`: UUIDv5 dùng namespace URL chuẩn UUID, name là JSON array compact UTF-8
  `[dag_id, dag_run.run_id]`. Cùng DagRun/task retry giữ nguyên UUID; DagRun mới là
  snapshot mới. Metadata giữ cả Airflow dag_id và chuỗi Airflow run_id để truy ngược.
- Validate 1–5 URLs HTTP(S), trim đầu/cuối, bỏ URL fragment; giữ nguyên path/query,
  không sắp xếp query hoặc coi hai URL redirect là cùng nguồn. Loại URL lặp chính xác
  sau chuẩn hóa, ghi requested_count và unique_input_count riêng.
- `page_id`: SHA-256 hex UTF-8 URL đã chuẩn hóa trên. Key dữ liệu là `(run_id,page_id)`;
  giữ cùng page_id khi response/redirect thay đổi giữa các snapshot.
- `content_hash`: SHA-256 response body bytes đã giải nén HTTP, trước decode/parse HTML;
  lưu charset/encoding để diễn giải raw_html. Không có body thì hash có thể NULL.
- `attempt_id`: định danh task attempt, truyền tường minh, phân biệt staging retry.
  Không dùng attempt_id làm run_id nghiệp vụ.

## Bảng dự kiến

| Bảng | Khóa dự kiến | Nội dung |
|---|---|---|
| bronze.web_pages | run_id,page_id | requested_url, final_url, fetched_at, http_status, content_type, content_hash, raw_html; bổ sung response_encoding, fetch_error để lưu lỗi trước HTTP |
| silver.documents | run_id,page_id | source_url, content_hash, title, clean_text, token_count, processed_at, transform_version; FK về Bronze |
| demo.word_counts | run_id,page_id,word | count > 0; giữ toàn bộ counts, FK về Silver |
| staging.documents | run_id,attempt_id,page_id | Đầu ra Spark trước publish |
| staging.word_counts | run_id,attempt_id,page_id,word | Đầu ra counts trước publish |
| quarantine.rejected_pages | run_id,page_id | reason, rejected_at, tham chiếu Bronze; không cần chép HTML |
| metadata.datasets | dataset_name,schema_version | owner, description; giữ phiên bản schema |
| metadata.pipeline_runs | run_id | Airflow identifiers, versions, active/completed attempt, publish_status |
| metadata.lineage | run_id,input_dataset,output_dataset | transform_version, rules_version, publish_status |
| quality.check_results | run_id,attempt_id,check_name,page_id hoặc run sentinel | status, observed_value, threshold, checked_at; phân biệt checks theo trang/theo run |
| telemetry.runs | run_id | start/end, status, duration, input/valid/rejected, total_tokens, error_summary |

Lưu ý nullable key quality: dùng check_id surrogate hoặc scope key không NULL khi
viết SQL; không dựa vào UNIQUE có NULL để chống duplicate. Lineage Bronze→job→Silver/demo
phải lưu job_name/application_id hoặc execution node để thấy bước xử lý thực tế.

## Crawl và transform

Mỗi URL đầu vào duy nhất có một bản ghi Bronze, kể cả HTTP lỗi, robots deny,
timeout hoặc content type không hỗ trợ; status/final_url/raw_html có thể NULL
khi chưa nhận response. Giữ error code rõ ràng để input=valid+rejected có nghĩa.
Giới hạn response dự kiến 2 MiB sau giải nén (đọc streaming và dừng khi quá mức),
timeout connect 5s/read 15s, tối đa 5 redirects, tối đa 2 lần retry cho lỗi tạm thời,
ít nhất 1s giữa requests cùng host hoặc lâu hơn theo robots crawl-delay.
Kiểm tra robots cho URL và redirect target, không bypass; lỗi robots xử lý bảo thủ
và báo reason. Crawler ngày 3 đã hiện thực các giới hạn này; delay/Retry-After vượt
ngân sách 30s thì dừng bằng reason riêng, không rút ngắn yêu cầu server.
404/410 robots cho phép tiếp tục; robots lỗi khác chặn fetch. Chi tiết/lệnh test
và trường hợp body đọc dở tại [day3.md](day3.md).

Parser `html.parser`, lấy body text, loại script/style/noscript/template, decode
entities, NFC rồi collapse whitespace. Body thiếu → reject; parser có thể sửa
HTML không hoàn chỉnh nên không đồng nhất “không phải XHTML hợp lệ” với “không parse được”.
Title lấy riêng, không thêm vào text. Không cam kết bỏ navigation/boilerplate.

Token: lowercase rồi NFC, mỗi chuỗi liên tiếp gồm chữ Unicode (category L) hoặc
decimal digit (Nd) là một token; dấu câu, underscore, ký hiệu khác là separator.
Giữ số, giữ dấu tiếng Việt; không stop words/stemming/lemmatization.
Ví dụ `dữ liệu` có 2 tokens. Extraction/tokenization trong Spark job, word-count
bằng Spark explode/groupBy/count. Dùng UDF để parse HTML được phép; không tính
toàn bộ kết quả ngoài Spark rồi khởi động Spark tượng trưng.

## Quality, retry và publish

Ngày 4 thêm rules reader, persisted quality/quarantine và partial/all-invalid;
schema/lệnh kiểm tra tại [day4.md](day4.md). Rules1.1.0, transform1.0.0.
Kết quả chạy thật xem STATUS. Catalog/lineage và telemetry còn thuộc ngày 5–6.

- Trang lỗi đã biết → quarantine theo page với reason; valid > 0 và rejected > 0
  → publish valid và status partial. Valid = 0 → quality evidence/quarantine giữ lại,
  không publish, Spark/Airflow thất bại. Lỗi DB/network/runtime/schema → system failure.
- Config dedup URL khác với lỗi duplicate key trong dữ liệu Spark: duplicate key
  còn lại là lỗi toàn run, fail trước publish; không tùy tiện chọn một bản ghi.
- Invariants: unique input = valid + rejected; counts > 0; sum(count) mỗi trang
  bằng Silver token_count; tổng run bằng telemetry.total_tokens.
- Trước Spark attempt mới: xóa staging cũ của run chưa publish, đánh dấu attempt mới.
  JDBC ghi bảng có unique keys; failure giữa các partition không tạo dữ liệu công bố.
  Chỉ đánh dấu staging_complete sau khi write + kiểm tra toàn bộ output thành công.
- Publisher dùng connection riêng, một transaction: khóa metadata row của run,
  xác nhận active attempt hoàn tất + quality pass, thay Silver/demo của run,
  ghi lineage/publish_status và commit. Failure → rollback cả transaction.
- Retry publish run đã công bố cùng versions/attempt hợp lệ là no-op. Run đã publish
  không tự recrawl/transform lại khi clear task; muốn dữ liệu mới tạo DagRun mới.
  Không mặc định JDBC append idempotent; không truncate cả lịch sử.
- Demo query join metadata publish_status='published'; partial ở quality_status,
  không đọc staging.
  Top-N chỉ giới hạn hiển thị: count DESC, word ASC theo PostgreSQL COLLATE "C"
  để có thứ tự byte UTF-8 ổn định với locale khác nhau.
- Callback/telemetry không được che lỗi upstream hoặc biến final DAG state thành success.

## Quyền dự kiến

Admin chỉ migration/seed; crawler ghi Bronze (INSERT/UPDATE cho retry, không DDL),
job đọc Bronze và ghi staging/quarantine/quality/telemetry cần thiết, publisher
đọc staging và publish Silver/demo/metadata, observer chỉ SELECT các view đã cấp.
Ngày 5 phải chứng minh truy cập bị từ chối, không chỉ nhìn GRANT statements.
Không dùng superuser làm kết nối pipeline thường ngày.
