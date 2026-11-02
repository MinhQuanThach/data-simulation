# Vận hành ngày 3: HTML web thật → Silver

Đây là mốc ngày 3. Từ ngày 4 đã có quality/quarantine và publish partial theo
[day4.md](day4.md); các đoạn mô tả fail toàn run bên dưới là hành vi lịch sử.

DAG `bronze_to_silver` nhận URL bằng cấu hình, dùng cùng Spark job/publisher của
ngày 2. Luồng hiện tại gồm năm task:
`validate_config → crawl_to_bronze → spark_clean_validate_count → publish_results → verify_output`.
Kết quả nghiệm thu và run IDs được ghi tại [STATUS](../STATUS.md).

## Khởi động và chạy URL

Trong PowerShell tại `D:\VCB\data-simulation`, với Docker Desktop Linux đang chạy:

```powershell
docker compose up -d --no-build --wait --wait-timeout 360
python scripts/day3.py migrate
python scripts/day3.py run --urls https://example.com --top-n 5
python scripts/day3.py run --urls https://www.python.org/about/ --top-n 5
```

Máy mới chưa có runtime: `python scripts/init-env.py` rồi `python scripts/day1.py up`
trước các lệnh ngày 3. Giữ nguyên pins và dependencies ngày 1; không cần build lại
image khi đã có runtime. `migrate` áp dụng SQL additive/rerunnable bằng admin riêng,
không reset volumes hoặc thay `.env`. Task dùng `lab_crawler`/`lab_spark` như ngày 2.

Trong Airflow UI http://localhost:8080, trigger `bronze_to_silver` bằng JSON:

```json
{"urls":["https://example.com","https://www.python.org/about/"],"top_n":5}
```

`urls` là list 1–5 phần tử **trước dedup**; trim đầu/cuối và bỏ fragment, giữ nguyên
path/query và thứ tự query. URL phải HTTP(S), có host, không credentials, whitespace
nội bộ hoặc control characters. Không sửa source khi đổi URL. `top_n` là integer
1–100, default 3; boolean không hợp lệ. Không truyền đồng thời `urls` và `fixture_case`.
Không truyền `urls` thì chạy fixture `valid_vi`; có thể chọn fixture empty/script_only
như ngày 2. `test_fail_after_documents_once` giữ lại cho test retry Spark.

## HTTP và robots

- User-Agent `BronzeSilverLab/1.0 (educational static HTML lab)`, crawl batch/depth 0;
  chỉ URL cấu hình, không crawl liên kết hoặc render JavaScript.
- Requests streaming: connect timeout 5s, read timeout 15s; response tối đa 2 MiB
  **sau giải nén gzip/deflate**, không chỉ dựa vào Content-Length.
- Tối đa 5 redirects; không để client tự follow. Mỗi đích redirect kiểm robots riêng.
  HTTP(S) và giới hạn URL vẫn áp dụng cho Location.
- Retry tối đa 2 lần cho lỗi kết nối/timeout lúc mở request hoặc HTTP
  429/500/502/503/504, backoff 1s/2s. Lỗi đọc body được lưu rõ và không giữ body dở.
  Retry-After số giây ≤30 được tôn trọng; giá trị lớn hơn hoặc HTTP-date dừng fetch
  với reason `retry_after_exceeds_budget`, không retry sớm hơn yêu cầu server.
- Ít nhất 1s giữa request cùng hostname, kể cả robots và redirects; tôn trọng
  Crawl-delay/Request-rate của robots. Delay >30s dừng bằng reason riêng để lab có
  giới hạn thời gian, không rút ngắn delay của website. Task crawl timeout 8 phút.
- Robots cache riêng từng origin trong một crawl batch. 404/410 nghĩa là không có
  robots; 401/403/5xx/network/decode body/size errors xử lý bảo thủ, không fetch page.
  Robots redirect tối đa 5 hops. Lưu văn bản robots và lịch sử HTTP để kiểm tra.
  Matcher hỗ trợ nhóm User-Agent, Allow/Disallow, longest match, wildcard `*`,
  end marker `$`, allow khi đồng hạng; urllib.robotparser đọc delay/rate.
- Chỉ HTML/XHTML có thể vào transform. HTTP lỗi, type khác, robots deny, timeout,
  body quá lớn hoặc decode lỗi có `fetch_error` riêng trong Bronze.
  HTTP charset ưu tiên; nếu không có dùng UnicodeDammit đọc BOM/meta/detect encoding.
  Decode lỗi hoặc ký tự NUL giữ bytes để debug, không ghi text không hợp lệ vào DB.
- Không dùng implicit `.netrc`/proxy environment credentials; TLS vẫn được xác minh.
  Các site login/paywall/bot challenge/JS/PDF nằm ngoài baseline.

## Snapshot và lưu trữ

`run_id` vẫn UUIDv5 theo `[dag_id, Airflow run_id]`; `page_id` SHA-256 URL sau
chuẩn hóa, không đổi sang hash của redirect target. Snapshot mới cần DagRun mới.

Migration [003_day3.sql](../sql/migrations/003_day3.sql) thêm:

- `bronze.fetch_details`, PK/FK `(run_id,page_id)`: `raw_body bytea`, `body_bytes`,
  `request_history jsonb`, `robots_history jsonb`, các response header chọn lọc và
  `crawler_version`. Bytes là body sau giải nén, trước decode; content_hash ở
  `bronze.web_pages` là SHA-256 của bytes này. Không lưu cookie/auth headers.
- `metadata.pipeline_runs`: `input_config`, `requested_count`, `unique_input_count`,
  `crawl_complete`. Đây là control metadata, chưa phải catalog/lineage ngày 5.

Giữ nguyên thứ tự cột Bronze cũ để các writer fixture/ngày 1–2 vẫn hoạt động.
`raw_html` là HTML đã decode; bytes gốc cho phép kiểm hash ngay cả khi encoding
không phải UTF-8. Với body vượt giới hạn hoặc đọc dở, không lưu/hash prefix như thể
đó là toàn response; bytes/html/hash có thể NULL, `body_bytes=0`.

Crawler giữ advisory lock cùng run với Spark/publisher. Cả batch Bronze/details
commit một transaction; exception hệ thống giữa batch rollback toàn bộ batch.
Một commit riêng đánh dấu crawl_complete; nếu chết ở khoảng giữa, retry kiểm tra
đủ keys/details và dùng lại snapshot đã commit. Replay không recrawl, kể cả khi
snapshot chứa lỗi nguồn. Config khác trên cùng run bị từ chối. Khi muốn thử lại
website sau khi nguồn thay đổi, tạo DagRun mới.

HTML/bytes không qua XCom; XCom chỉ UUID, config và thống kê nhỏ. Extraction,
tokenization và counts vẫn chạy trong Spark standalone executor qua JDBC. Top-N
áp dụng **từng trang**, count DESC rồi word COLLATE "C" ASC; toàn bộ counts giữ lại.
Verify web kiểm bytes/hash/source, số trang, token sums và versions; runner còn
đếm độc lập toàn bộ tokens từ Silver bằng Python stdlib để đối chiếu counts.

Ngày 3 giữ policy ngày 2: có trang lỗi thì Spark fail toàn run, publisher bị chặn,
không có Silver/demo của run. Chưa persisted quality/quarantine hoặc publish partial;
đây là công việc ngày 4. Lỗi DB/code không được đổi thành lỗi URL hoặc success.

## Nghiệm thu và xem bằng chứng

```powershell
python scripts/day3.py accept
$day3 = Get-Content docs/evidence/day3/latest.json -Raw | ConvertFrom-Json
python scripts/day3.py inspect --run-id $day3.first_run --top-n 5
docker compose cp sql/queries/day2_top_n.sql lab-db:/tmp/top_n.sql
docker compose exec -T lab-db psql -X -v ON_ERROR_STOP=1 -U lab_admin -d lab -v "run_id=$($day3.multi_run)" -v top_n=2 -f /tmp/top_n.sql
```

URLs nghiệm thu nằm trong [day3_acceptance.json](../config/day3_acceptance.json).
Nếu nguồn đổi hành vi, chọn URL HTML công khai khác trong config; không tắt robots
hoặc bỏ TLS để đạt PASS. Runner lưu bằng chứng của run lỗi trước khi thoát.
`run` yêu cầu success và sẽ exit nonzero khi URL lỗi; `accept` có case riêng kỳ vọng
DAG failed, có reason và public rows=0. Không tự đổi các DagRun đỏ thành success.

Bộ `accept` chạy unit tests fixture và HTTP server cục bộ; kiểm rollback/retry Bronze
bằng DB thật; inventory driver/worker/pip/JDBC; hai URL web tuần tự không đổi source;
run nhiều trang kèm URL duplicate; clear đúng logical date để replay snapshot web;
URL lỗi; fixture regression qua năm task. Kiểm bảy service healthy cuối bộ test.
HTTP server cục bộ chỉ là test deterministic, được ghi rõ riêng với evidence web.

`docs/evidence/day3/latest.json` chỉ xuất hiện/cập nhật khi toàn bộ nghiệm thu PASS.
Mỗi run có task logs, executor logs, report/source hashes, SQL output (Bronze/HTTP,
Silver, toàn counts, top-N), `.body` bytes gốc và `.html` đã decode. Replay lưu riêng.
Evidence local bị Git ignore, có runner/tests để tái tạo. Không suy ra run hiện tại
PASS từ `latest.json` lịch sử: luôn kiểm timestamp và source hashes.

Nếu lỗi: xem `initial/task-logs.txt`, `initial/sql-output.json`, `docker compose ps -a`
và `docker compose exec -T airflow-scheduler airflow dags list-import-errors`.
Không cài stack lên host, không prune/reset/down -v. Dừng giữ dữ liệu:
`python scripts/day1.py stop`.

Nguồn chính thức đã đối chiếu: [Requests streaming/timeouts](https://requests.readthedocs.io/en/latest/user/advanced/),
[urllib.robotparser](https://docs.python.org/3.11/library/urllib.robotparser.html),
[RFC 9309 robots](https://www.rfc-editor.org/rfc/rfc9309.html).
Chính sách conservative và ngân sách 30s nêu trên là lựa chọn của lab.
