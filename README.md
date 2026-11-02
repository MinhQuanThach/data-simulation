# Bronze → Silver trong 7 ngày

Lab local để học tương tác của 7 block Data Engineering bằng HTML thật.
Đặc tả gốc: [bronze_silver_7day_project_prompt.md](bronze_silver_7day_project_prompt.md).

**Ngày 1–5 đã hoàn thành, PASS ngày 5 lúc 11:03:38 ngày 02/11/2026 Asia/Saigon**:
31 unit tests, 57 phép thử allow/deny, catalog/lineage, atomic publish và retry/replay fixture/web.
Xem [vận hành ngày 5](docs/day5.md) và [STATUS](STATUS.md). Chạy `python scripts/day5.py accept` trên runtime
đã có; `python scripts/day5.py inspect --run-id <UUID>` query lineage bằng observer.

Mốc lịch sử 2026-11-01: ngày 1–4 PASS. Ngày6–7 chưa triển khai.
Ngày4 accept PASS23:37:23: 26unit tests, quality/quarantine/partial, all-invalid/system
failure, retry/replay/atomic publish và web partial thật; bằng chứng tại STATUS.
Rules reader, quality/quarantine và partial/all-invalid: [vận hành ngày 4](docs/day4.md).
Kết quả mới nhất luôn xem [STATUS](STATUS.md).
Đã có Compose/runtime Airflow–Spark, DAG JDBC smoke và DAG fixture → Spark clean/
tokenize/count → transaction publish Silver/demo. Text/counts, retry task thật,
replay cùng DagRun và rollback publish đã kiểm tra; kết quả đầy đủ tại [STATUS](STATUS.md).
Ngày 3 đã thêm crawler nhận `urls` từ DAG conf; xem kết quả xác minh tại STATUS.

Đọc theo thứ tự:

1. [Hướng dẫn môi trường Windows/Docker](docs/environment.md): kết quả máy thật,
   việc cần làm thủ công, lệnh xác nhận.
2. [STATUS](STATUS.md): bằng chứng đã có và các bước chưa kiểm tra.
3. [Phiên bản](docs/versions.md): image tags/digests, dependency pins, giới hạn xác minh.
4. [Kiến trúc](docs/architecture.md) và [hợp đồng dữ liệu](docs/data_dictionary.md).
5. [PLAN](PLAN.md): mục tiêu, file dự kiến và nghiệm thu từng ngày.
6. [Vận hành ngày 1](docs/day1.md): pull/build/init, UI, smoke/query và xử lý lỗi.
7. [Vận hành ngày 2](docs/day2.md): fixture/Silver, migration, nghiệm thu retry/rollback/top-N.
8. [Vận hành ngày 3](docs/day3.md): URL configurable, crawler/robots, bytes Bronze,
   test HTTP thật, replay và evidence web.
9. [Vận hành ngày 4](docs/day4.md): rules reader, quality/quarantine, policy partial,
   all-invalid/system failure và nghiệm thu retry/invariants.

Khởi động và kiểm tra ngày 1:

```powershell
python scripts/init-env.py
python scripts/day1.py up
python scripts/day1.py runtime
python scripts/day1.py smoke
python scripts/day1.py status
```

Airflow http://localhost:8080, Spark master http://localhost:8081, worker
http://localhost:8082. Tài khoản Airflow trong `.env` local, không commit hoặc gửi
file này. Script smoke lưu report, SQL output và executor log trong
`docs/evidence/day1/`; `latest.json` chỉ xuất hiện khi đủ kiểm tra PASS.
Dừng và giữ dữ liệu: `python scripts/day1.py stop`.

Kiểm tra phần chuẩn bị, không cài dependency:

```powershell
Set-Location D:\VCB\data-simulation
python scripts/check-preparation.py
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/preflight.ps1
```

Lệnh preflight chỉ đọc môi trường, không sửa Docker/WSL, không cài phần mềm,
không đọc giá trị trong `.env`. Chạy từ PowerShell của bạn nếu sandbox từ chối
truy cập Docker. Thêm `-Online` để thử registry/PyPI; thêm `-InspectImages`
để kiểm tra manifest các image đã chọn (không pull layers).

Luồng mục tiêu: Airflow → crawler → Bronze PostgreSQL → Spark standalone →
quality/staging → transaction publish → Silver + demo word-count.
Grafana đọc telemetry qua role chỉ đọc từ ngày 6.

Giới hạn: 1–5 URL/run, batch, depth 0, static HTML công khai; không đăng nhập,
paywall, JavaScript rendering, PDF, streaming, Gold hoặc ML. PostgreSQL là
storage quan hệ; metadata tự xây; quality dùng Spark; tokenization đơn giản.
Không có DataHub, lakehouse, dbt, Prometheus hay OpenTelemetry trong baseline.

Chạy ngày 2 trên runtime ngày 1 đã build:

```powershell
docker compose up -d --no-build --wait --wait-timeout 360
python scripts/day2.py migrate
python scripts/day2.py run
python scripts/day2.py accept
```

`run` là luồng bình thường. `accept` tạo thêm các DagRun lỗi có chủ đích để kiểm
tra retry và chặn text rỗng; `latest.json` chỉ ghi sau khi toàn bộ checks PASS.
Xem [docs/day2.md](docs/day2.md) để query kết quả và đọc bằng chứng.

Chạy URL web thật trên runtime đã có:

```powershell
python scripts/day3.py migrate
python scripts/day3.py run --urls https://example.com --top-n 5
python scripts/day3.py run --urls https://www.python.org/about/ --top-n 5
python scripts/day3.py accept
```

DAG hiện có năm task validate/crawl/Spark/publish/verify. Bỏ `urls` để chạy lại
fixture. `top_n` giới hạn từng trang, toàn bộ counts giữ lại. Mỗi URL duy nhất có
Bronze row kể cả lỗi; ngày 3 vẫn fail toàn run khi có trang lỗi, không publish.
Hành vi trên là mốc ngày 3; ngày 4 thay bằng quarantine/publish phần hợp lệ.
Catalog/lineage đã có code ngày 5; Grafana còn chờ ngày 6.
Worker dùng chung image Airflow để đồng nhất Python 3.11; đây là giản lược cho lab.
Hai PostgreSQL giữ volume riêng; admin chỉ init, task dùng role crawler/job riêng.
Ngày 5 publisher có identity riêng; allow/deny tests và phạm vi quyền tại docs/day5.md.
Không xóa volume để cold start; ngày 7 dùng project/volume thử nghiệm riêng.

Repository public: [MinhQuanThach/data-simulation](https://github.com/MinhQuanThach/data-simulation),
nhánh chính `main`, remote `origin`. `.env.example` chỉ chứa placeholder.
Không commit `.env`, log hoặc binary tải về.
Nếu Git báo ownership khác tài khoản sandbox, xem cách dùng ngoại lệ theo lệnh
trong [hướng dẫn môi trường](docs/environment.md#git-và-ownership-của-sandbox).
