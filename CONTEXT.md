# Context chuyển session

- Ngày tham chiếu: 2026-10-29, Asia/Saigon. Giai đoạn hiện tại: chuẩn bị trước ngày 1.
- User đã cho phép thực hiện các bước chuẩn bị; chưa yêu cầu triển khai task ngày 1.
- Đã đọc toàn bộ đặc tả và hai ảnh `images/data-flow.png`, `images/medallion.png`.
- Mục tiêu: lab 7 ngày, khoảng 3–4 giờ/ngày, Airflow + Spark bắt buộc, dữ liệu nhỏ.
- Scope: HTML nguồn Bronze → text chuẩn hóa Silver → demo word-count. Chưa Gold/ML/BI.
- Stack: hai PostgreSQL service, Airflow LocalExecutor, Spark standalone master/worker,
  Grafana ngày 6; các block khác hiện thực bằng code và bảng PostgreSQL.
- Windows 11 Home 10.0.26200 x64, i5-1240P, 16 CPU logic, RAM 31.69 GiB.
- Docker Desktop 4.75.0, Engine 29.5.2, Compose plugin 5.1.3, context `desktop-linux`,
  daemon Linux/amd64. Ubuntu WSL2 đã cài; đang stopped khi kiểm tra; docker-desktop running.
- Docker đang nhận 3.83 GiB/2 CPU. Khuyến nghị đổi 8 GiB/4 CPU trước ngày 1.
- Đã đọc riêng khóa tài nguyên .wslconfig: memory=4GB, processors=2, swap=8GB.
  User chỉ cần sửa memory/processors, giữ swap/cấu hình khác; chưa sửa file ngoài workspace.
- Đĩa trống: C 26.65, D 18.97, E 37.56 GiB. Chưa xác minh host path của Docker disk image.
- Sandbox báo access denied với CIM, WSL, `.docker/config.json`, Docker named pipe.
  Read-only ngoài sandbox đã thành công. Không suy ra Docker bị hỏng từ lỗi sandbox.
- Git init chạy bằng CodexSandboxOffline nên Git dưới DELL báo dubious ownership.
  Dùng `git -c safe.directory=D:/VCB/data-simulation ...` theo lệnh; không sửa global/ACL.
- Docker Hub registry trả HTTP 401 challenge bình thường; PyPI HTTP 200;
  manifest bốn image có linux/amd64. Chưa pull layers/build/run containers.
- Source pins tại `config/stack.json`; constraints và requirements tại `requirements/`.
  Airflow 3.3.2, Spark 4.0.2, provider 5.5.0, Python minor 3.11, Java major 17,
  PostgreSQL 16.15, JDBC 42.7.14, Grafana OSS 12.4.2. Runtime chưa kiểm chứng.
- Provider 5.5.0 chọn để dùng dependency PySpark đầy đủ cho spark-submit;
  project constraints khác upstream ở provider và các pin PySpark/Py4J bổ sung.
- Shared custom runtime dự kiến dựa trên Airflow Python 3.11, stage Spark + JRE
  từ apache/spark. Dùng cùng runtime cho scheduler/master/worker để đồng nhất Python.
  Ghi rõ sự giản lược này; chưa build, không khẳng định base images có cùng Python.
- Driver chạy tại airflow-scheduler; host `airflow-scheduler`, bind `0.0.0.0`,
  driver port 39001, block manager 39002; Spark client mode, master `spark://spark-master:7077`.
- `max_active_runs=1`, Airflow parallelism/task Spark pool 1 để một driver tại một thời điểm.
- run_id UUIDv5 từ dag_id + Airflow dag_run.run_id, ổn định qua retry; page_id SHA-256 URL
  sau trim/bỏ fragment; content_hash từ response body bytes. Chi tiết trong data dictionary.
- Staging theo run + attempt; cleanup trước retry; publish Silver/demo/metadata trong transaction.
- Transform/rules version 1.0.0; body text, bỏ script/style/noscript/template, NFC,
  collapse whitespace; lowercase, giữ chữ Unicode + decimal digits; không stop words/stemming.
- Fixture viết tay: 12 tokens/8 từ khác nhau; không phải dữ liệu crawl.
- Secret local dự kiến ở `.env`, chỉ template đã có; chưa tạo secret thật hoặc kết nối.
- Chưa thử URL thật; chưa có run_id/application_id thực tế; chưa có block chạy cùng nhau.
- Lệnh hiện dùng: `python scripts/check-preparation.py`,
  `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/preflight.ps1 -Online -InspectImages`.
- Hai lệnh kiểm tra trên đã pass đúng phạm vi (preflight còn cảnh báo tài nguyên).
- Lệnh khởi động/trigger/query/dừng pipeline: chưa có, sẽ triển khai ngày 1.
- Bước tiếp: user điều chỉnh tài nguyên Docker; kiểm tra lại; thực hiện PLAN ngày 1.
