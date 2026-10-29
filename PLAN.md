# Kế hoạch thực hiện

Giai đoạn chuẩn bị không tính là ngày 1 hoàn thành. Mỗi ngày trước khi sửa code:
nêu lại mục tiêu, file dự kiến, tiêu chí xong; cuối ngày cập nhật CONTEXT/STATUS,
lệnh chạy lại, bằng chứng, lỗi và việc còn thiếu. Không tự đánh dấu ngày sau là xong.

| Giai đoạn | Mục tiêu | File/phạm vi dự kiến | Tiêu chí hoàn thành |
|---|---|---|---|
| Chuẩn bị | Môi trường, pins, kiến trúc, hợp đồng dữ liệu, fixture | Tài liệu gốc root, docs, config, requirements, fixtures, scripts, skeleton | Evidence chỉ đọc; tài liệu hướng dẫn thủ công; offline check pass; ghi rõ runtime pending |
| 1 | Kết nối Storage–Orchestration–Execution thật | compose.yaml, docker/*/Dockerfile, requirements lock, sql/init, dags/smoke_bronze.py, src/jobs/smoke_jdbc.py, scripts vận hành | Services healthy, worker registered, DAG success, Spark app/executor log, SQL staging có output đúng fixture |
| 2 | Fixture Bronze → Silver → counts | src/jobs/clean_validate_count.py, src/common, sql/migrations, sql/queries, publish task, dags/bronze_to_silver.py, tests fixture/retry | Clean text/counts khớp expected; check rỗng; transaction publish; retry cùng run không trùng |
| 3 | Crawl HTML thật, URL từ cấu hình | src/crawler, validate_config, DAG conf, test crawl/error | Hai URL lần lượt không sửa source; HTML nguồn/HTTP metadata thật; URL lỗi được nhận diện |
| 4 | Quality và quarantine | Rules reader, config/quality_rules.yaml, quality/quarantine SQL, invariant checks | Valid/empty/script-only/duplicate/HTTP error; partial rõ; all-invalid fail không publish; counts/token khớp |
| 5 | Access + metadata | Roles/GRANT migrations, config/datasets.yaml, lineage code, permission tests | Role allow/deny có bằng chứng; admin tách riêng; truy vấn lineage/version theo run |
| 6 | Monitoring thật | telemetry code, grafana/provisioning, grafana/dashboards | Thành công/lỗi khác nhau; liên kết run_id; dashboard tái provision sau restart |
| 7 | Tái lập và bàn giao | scripts nghiệm thu, README/docs/STATUS, transcripts | Cold start project/volume riêng; fixture, ≥2 URL thật, lỗi, retry, restart; checklist đặc tả có bằng chứng |

Ngày 1 cần đi theo thứ tự:

1. Kiểm tra lại Docker 8 GiB/4 CPU và dung lượng trên ổ chứa Docker disk image.
2. Pull image nguồn đã pin; build shared runtime; chạy `pip check`, kiểm tra Java,
   Python và Spark trên driver/worker; xuất dependency lock/inventory đầy đủ.
3. Viết Compose LocalExecutor với api-server/scheduler/dag-processor/init,
   hai PostgreSQL, Spark master/worker; healthchecks và named volumes.
4. Tạo secrets local, migration/seed admin riêng, tài khoản job không là superuser;
   quyền cơ bản đủ dùng ngày 1, ma trận đầy đủ/nghiệm thu deny ngày 5.
5. Seed fixture hợp lệ, kích hoạt smoke DAG và kiểm tra SQL staging.
6. Lưu application_id, executor log, run_id và kết quả SQL. Nếu bị chặn,
   giữ implementation có thể làm, ghi lệnh để user chạy, không báo E2E pass.

Chưa thêm Grafana vào ngày 1; chưa crawler trước khi fixture baseline ổn định.
