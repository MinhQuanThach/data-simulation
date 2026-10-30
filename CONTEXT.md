# Context chuyển session

Cập nhật 2026-10-30, Asia/Saigon. **Ngày 1 đã hoàn thành có evidence PASS; ngày tiếp theo là ngày 2.**
User đã yêu cầu đọc toàn bộ đặc tả/Markdown/repo và thực sự triển khai ngày 1;
không chỉ hướng dẫn, không đánh dấu ngày khác hoàn thành. Không có yêu cầu commit/push.

- Đặc tả gốc: bronze_silver_7day_project_prompt.md. Lab 7 ngày, Airflow + Spark bắt buộc.
- Ranh giới: HTML nguồn Bronze → text chuẩn hóa Silver → word-count schema demo.
  Chưa Gold/ML/BI; PostgreSQL là storage quan hệ cho lab nhỏ, không gọi là lakehouse.
- Scope cuối tuần: static HTML công khai, 1–5 URLs/run, depth 0, batch; chưa JS/paywall/login/PDF.
- Đã đọc toàn bộ Markdown. Tài liệu vận hành mới: docs/day1.md; chi tiết bằng chứng: STATUS.md.
- Host Windows 11 Home 10.0.26200 x64, i5-1240P, 16 CPU logic, 31.69 GiB RAM.
  Docker Desktop 4.75.0, Engine 29.5.2, Compose plugin 5.1.3, context desktop-linux.
- Docker Linux/amd64 nhận 4 CPU/7.76 GiB; WSL cấu hình trước đó 8GB/4 CPU/swap 8GB.
  Docker data E:\Docker\WSL. Preflight ngày 1: C 26.38, D 18.97, E 36.25 GiB trống
  lúc đo giữa quá trình pull; không coi đây là dung lượng sau build. Xem evidence preflight.
- Sandbox exec setup refresh error; các lệnh cần thiết chạy ngoài sandbox đã được auto-review
  chấp thuận. Không sửa môi trường/ACL; không suy ra Docker hỏng từ lỗi sandbox.
- Git khác ownership: dùng git -c safe.directory=D:/VCB/data-simulation ... theo lệnh;
  không sửa global config hoặc ACL. `.git` chỉ đọc trong sandbox. Working tree có code/docs mới.
- Pins tại config/stack.json, Dockerfile và requirements/. Airflow 3.3.2, provider Spark 5.5.0,
  standard/common-compat 1.19.0, Spark/PySpark 4.0.2, Py4J 0.10.9.9, Scala 2.13.16,
  Python **3.11.16** driver/worker/executor, Temurin Java **17.0.17+10**,
  PostgreSQL **16.15**, JDBC **42.7.14**. Grafana OSS 12.4.2 chỉ pin nguồn, chưa chạy.
- Ba base images đã pull đúng digest linux/amd64. Custom image bronze-silver-runtime:day1
  digest sha256:24e02337863a2f5d423a587a14bf6d1bb059b76b89a881f8d0366f02fe1b9bb4.
  Inventory thực tế docs/evidence/day1/runtime.json; freeze 446 dòng ở
  requirements/runtime-freeze-py311.txt là inventory, không phải requirements tái cài.
- Build đa stage lấy Spark/JRE từ apache/spark, Python từ apache/airflow. Cùng image cho
  Airflow/master/worker để đồng nhất Python. Worker mang cả dependency Airflow là giản lược có chủ đích.
- PySpark sdist single-stream bị throttling. Helper docker/airflow/fetch_pyspark.py tải ranges
  có giới hạn và kiểm SHA-256 toàn file chính thức; không đổi version, không bypass TLS,
  không tải dependency lúc task chạy. JDBC cũng khóa checksum tại build. Blocker mạng đã xử lý.
- compose.yaml: hai DB riêng, airflow-init/api-server/scheduler/dag-processor LocalExecutor,
  Spark master/worker; volumes-init và lab-init chạy một lần. 7 service chạy lâu đều healthy.
  Không triggerer/Celery/Redis/Grafana. Một bridge network; DB/log/auth/Spark work named volumes.
- UI bind 127.0.0.1: Airflow 8080, master 8081, worker 8082. DB không publish host port.
  Driver trong airflow-scheduler; host airflow-scheduler, bind 0.0.0.0, ports 39001/39002;
  master spark://spark-master:7077, client deploy mode. Không Docker socket/exec trong DAG.
- Airflow parallelism=1, Spark pool spark_single=1, DAG max_active_runs=1; worker 2 cores/2g,
  smoke executor 1 core/1g, driver 1g; speculation false, task.maxFailures=1.
- Secrets local `.env` sinh ngẫu nhiên bằng scripts/init-env.py, chạy lặp giữ nguyên file.
  SimpleAuthManager password file precreate trong airflow-auth volume; không in mật khẩu startup.
  Authenticated UI/DAG API đã HTTP 200. JWT/API signing key dùng chung cho các Airflow service.
  Không đọc/in/commit secrets. Không xóa `.env` rồi sinh mới khi DB volumes đang giữ password cũ.
- LAB admin chỉ lab-init migration/seed và operator query. Pipeline dùng lab_crawler/lab_spark,
  đã xác nhận không superuser/CREATEDB/CREATEROLE. Crawler ngày 1 có SELECT/INSERT Bronze để
  nhân fixture gốc; Spark SELECT Bronze, SELECT/INSERT/DELETE staging. Ma trận quyền đầy đủ ngày 5.
- sql/init/001_day1.sql hiện thực bronze.web_pages (UUID run_id, PK run/page, HTTP/source fields)
  và staging.smoke_results (run_id text, PK run/page, byte size/hash/app ID/executor Python/time).
  Staging này chỉ chẩn đoán JDBC, không phải Silver/counts staging ngày 2.
- Fixture gốc admin seed run UUID 00000000-0000-0000-0000-000000000001,
  requested_url https://fixture.invalid/valid-vi, page_id SHA-256 URL. `.invalid` không được crawl.
  HTML gốc 425 bytes, hash 3cb794bd70768c23cc1a5eb470678df94713fe157e5d860b44277669ff2b64a0.
- DAG dags/smoke_bronze.py: prepare_fixture → spark_jdbc_smoke → verify_staging.
  Prepare kiểm worker registered và nhân fixture sang run hiện tại; SparkSubmitOperator chạy
  src/jobs/smoke_jdbc.py, đọc/ghi JDBC thật, Python UDF ghi version thực trên executor;
  verify so SQL với file fixture. XCom chỉ có UUID/thống kê nhỏ, không HTML/DataFrame.
- run_id = UUIDv5 namespace URL, compact JSON [dag_id, Airflow run_id]. Task retry giữ UUID;
  DagRun mới là snapshot mới. page_id hash URL trim/bỏ fragment; content_hash hash body bytes.
- Spark smoke xóa staging riêng run trước mỗi attempt rồi append có PK; chưa nghiệm thu retry.
  Thiết kế ngày 2: staging theo run+attempt, đánh dấu hoàn tất, transaction publish Silver/demo/
  metadata; retry published run no-op; không truncate lịch sử hoặc publish nửa kết quả.
- Transform/rules 1.0.0 vẫn là hợp đồng chưa thực thi: body text; bỏ script/style/noscript/template,
  entities/NFC/whitespace; lowercase Unicode letters + decimal digits, giữ số, không stopwords/
  stemming. fixtures/expected.json oracle: 12 tokens, 8 words, top-3 chào/dữ/liệu.
- Hai smoke DagRuns đều success, 3 task/run try 1. Run đầu UUID
  625dcd74-33d0-5a6b-9795-7f9d9d905d8f / app-20261030024425-0000.
  Run cuối Airflow day1_20261030T024720408784Z, UUID ff1a7cb4-6fb7-5712-ba50-cfe99bae83fd,
  app-20261030024732-0001, khoảng 09:47 Asia/Saigon ngày 2026-10-30.
  Mỗi run 1 SQL row đúng 425 bytes/hash, executor log có Running task.
- Evidence gần nhất docs/evidence/day1/latest.json → report/SQL/task/executor/master state;
  runtime.json và supplementary.json có pip/source hashes/auth/roles/PG facts. Không có URL web thật.
- Đã nối Storage ↔ Execution ↔ Orchestration; basic Access có role riêng. Chưa transformation,
  quality/quarantine, metadata/lineage, transaction publish, telemetry/Grafana hoặc cold-start/persistence test.
- Không còn blocker/nghĩa vụ sửa tay ngày 1. Stack đang chạy. Không xóa volume, prune hoặc reset.

Lệnh tiếp tục tại D:\VCB\data-simulation:

```powershell
python scripts/init-env.py
python scripts/day1.py up
python scripts/day1.py runtime
python scripts/day1.py smoke
python scripts/day1.py status
docker compose cp sql/queries/day1_smoke.sql lab-db:/tmp/day1_smoke.sql
docker compose exec -T lab-db psql -X -v ON_ERROR_STOP=1 -U lab_admin -d lab -f /tmp/day1_smoke.sql
python scripts/day1.py stop
```

Bước tiếp theo: triển khai **ngày 2**, giữ ngày 1 làm smoke baseline. Tạo Spark clean/
tokenize/explode/groupBy/count, schema staging/Silver/demo và transaction publish, verify
fixture theo expected.json, thử retry cùng run không duplicate. Cập nhật CONTEXT/PLAN/STATUS
với bằng chứng thực tế; không báo crawler/quality/monitoring đã xong trước ngày tương ứng.
