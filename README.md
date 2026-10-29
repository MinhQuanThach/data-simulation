# Bronze → Silver trong 7 ngày

Lab local để học tương tác của 7 block Data Engineering bằng HTML thật.
Đặc tả gốc: [bronze_silver_7day_project_prompt.md](bronze_silver_7day_project_prompt.md).

**Trạng thái 2026-10-29: chuẩn bị trước ngày 1; chưa triển khai pipeline.**
Chưa có `compose.yaml`, Dockerfile hoặc DAG hoạt động. Không có kết quả
end-to-end, dữ liệu crawl thật hay Spark executor log trong bước này.

Đọc theo thứ tự:

1. [Hướng dẫn môi trường Windows/Docker](docs/environment.md): kết quả máy thật,
   việc cần làm thủ công, lệnh xác nhận.
2. [STATUS](STATUS.md): bằng chứng đã có và các bước chưa kiểm tra.
3. [Phiên bản](docs/versions.md): image tags/digests, dependency pins, giới hạn xác minh.
4. [Kiến trúc](docs/architecture.md) và [hợp đồng dữ liệu](docs/data_dictionary.md).
5. [PLAN](PLAN.md): mục tiêu, file dự kiến và nghiệm thu từng ngày.

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

Các thư mục skeleton có `.gitkeep` chưa chứa implementation. Chỉ tạo file chạy
được khi đến ngày tương ứng; không dùng file giả trả success để đại diện tiến độ.
Lệnh pull/build/start/init/trigger/query/stop sẽ bổ sung và chạy thử ở ngày 1.
Chưa chạy `docker compose up` khi chưa có Compose. Không xóa volume để cold start;
ngày 7 dùng project/volume thử nghiệm riêng.

Repository public: [MinhQuanThach/data-simulation](https://github.com/MinhQuanThach/data-simulation),
nhánh chính `main`, remote `origin`. `.env.example` chỉ chứa placeholder.
Không commit `.env`, log hoặc binary tải về.
Nếu Git báo ownership khác tài khoản sandbox, xem cách dùng ngoại lệ theo lệnh
trong [hướng dẫn môi trường](docs/environment.md#git-và-ownership-của-sandbox).
