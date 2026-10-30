# Kế hoạch thực hiện

Cập nhật 2026-10-30: **chuẩn bị và ngày 1 đã hoàn thành**. Hai smoke DagRuns PASS,
7 service healthy, worker ALIVE, runtime driver/worker khớp, có SQL output và executor log.
Evidence tại [STATUS](STATUS.md) và [latest.json](docs/evidence/day1/latest.json).
**Ngày 2–7 chưa triển khai**; bước tiếp theo là ngày 2, không tự coi các ngày sau đã xong.

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

Ngày 1 đã thực hiện và kiểm chứng theo thứ tự:

1. [x] Docker Linux/amd64, 7.76 GiB/4 CPU; preflight không warnings, xác nhận đĩa host.
2. [x] Pull đúng digest; build shared runtime; pip check/import/Java/Spark PASS;
   Python 3.11.16/Java 17.0.17; xuất inventory 446 dòng và image digest.
3. [x] Compose LocalExecutor api-server/scheduler/dag-processor/init,
   hai PostgreSQL, master/worker; healthchecks/volumes; 7 service healthy.
4. [x] Secrets local, admin init riêng, lab_crawler/lab_spark không superuser;
   UI auth HTTP 200. Ma trận quyền đầy đủ/nghiệm thu deny vẫn thuộc ngày 5.
5. [x] Seed fixture và hai DagRuns thật; 3 task/run success; SQL 1 row/run,
   425 bytes/hash khớp fixture; app IDs khác nhau theo snapshot.
6. [x] Lưu run/application IDs, Airflow task log, executor log, master JSON, SQL output;
   không còn blocker ngày 1. Lệnh tái chạy đã có tại docs/day1.md.

Chưa thêm Grafana vào ngày 1; chưa crawler trước khi fixture baseline ổn định.

Ngày 2 cần làm tiếp:

1. Migration staging.documents/staging.word_counts, silver.documents/demo.word_counts
   và metadata/publish tối thiểu để transaction/lineage có điểm mở rộng về sau.
2. Spark parse body text, Unicode NFC/tokenize theo config hợp đồng; explode/groupBy/count;
   kiểm tra clean_text không rỗng, giữ đủ counts; không tính counts bằng Python ngoài Spark.
3. Retry cùng run_id: cleanup attempt có kiểm soát, publish một transaction,
   run đã publish cùng versions/attempt hợp lệ no-op; lỗi không để lộ nửa kết quả.
4. DAG fixture Bronze → Spark → publish → verify; oracle 12 tokens/8 words/top-3;
   chạy và lưu SQL/log bằng chứng, cập nhật tài liệu. Chưa crawl URL thật trong ngày 2.
