# Kế hoạch thực hiện

Cập nhật 2026-11-01: **ngày 1–4 đã hoàn thành, nghiệm thu PASS**. Ngày 2 có fixture
Bronze → Spark standalone → Silver/demo, oracle 12 tokens/8 từ, retry task thật,
replay cùng run, rollback transaction và empty/script-only rejection PASS.
7 service healthy, runtime driver/worker khớp; có SQL output và executor logs.
Evidence tại [STATUS](STATUS.md), [ngày 1](docs/evidence/day1/latest.json),
[ngày 2](docs/evidence/day2/latest.json), [ngày 3](docs/evidence/day3/latest.json).
Ngày 3 nghiệm thu lúc 19:51:30; hai URL thật, multi/dedup, web replay, URL404 không
publish, fixture regression và targeted Spark retry PASS. **Ngày 5–7 chưa triển khai**.

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

Ngày 2 đã thực hiện và kiểm chứng:

1. [x] Migration 002_day2.sql: staging/Silver/demo + PK/FK/checks, published view,
   control metadata tối thiểu; init-lab áp dụng lại không xóa lịch sử.
2. [x] Spark parse body/NFC/tokenize theo hợp đồng 1.0.0; explode/groupBy/count,
   5 unit tests Linux và full oracle PASS. YAML policy reader vẫn thuộc ngày 4.
3. [x] Task retry thật sau ghi dở staging; cleanup attempt riêng run; publisher chặn
   incomplete; attempt 2 output đúng, không duplicate. Có log từng app/run/attempt.
4. [x] Publish một transaction; connection khác không thấy uncommitted; lỗi giữa publish
   rollback cả Silver/demo/metadata; retry publish no-op sau success.
5. [x] Clear đúng logical date cùng DagRun đã publish, đủ 4 task chạy lại success,
   snapshot/timestamps/app ID không đổi. DagRun mới giữ snapshot mới riêng run_id.
6. [x] Empty/script-only fail, downstream upstream_failed, không public rows;
   top-N SQL chạy thật, không cắt toàn bộ counts. 7 service healthy sau nghiệm thu.
7. [x] docs/day2.md, README, data dictionary/architecture/versions/fixtures docs và
   CONTEXT/PLAN/STATUS cập nhật. Runner: python scripts/day2.py accept.

Chưa làm crawler, quarantine/partial, full catalog/lineage/roles hoặc dashboard trong ngày 2.
Ngày 2 tạm dùng chung lab_spark cho job/publisher; tách identity/test deny ở ngày 5.

Ngày 3 đã triển khai và kiểm chứng:

1. [x] Validate cấu hình urls/top_n, chuẩn hóa/dedup URL và run/page IDs theo hợp đồng;
   giữ đường fixture để regression, không dùng oracle fixture để verify trang web.
2. [x] Crawler HTTP: robots, timeout, response size sau giải nén, redirects/retries giới hạn,
   rate limit; Bronze lưu source bytes/hash/encoding/HTTP fields và lỗi theo từng URL.
3. [x] DAG nhận URLs từ dag_run.conf, crawl → Spark hiện có → publish → verify invariants;
   system failure vẫn fail thật, chưa coi partial/quarantine ngày 4 đã hoàn tất.
4. [x] Chạy URL HTML công khai rồi đổi sang URL thứ hai bằng config, không sửa source;
   lưu bằng chứng HTTP/HTML thật/Silver/top-N, test URL lỗi báo rõ.
5. [x] Hoàn tất fixture/retry regression và chốt latest.json PASS; cập nhật docs và
   CONTEXT/PLAN/STATUS bằng kết quả đã đo; không tự triển khai ngày 4.

Files ngày 3: `src/crawler`, migration `003_day3.sql`, DAG năm task,
`scripts/day3.py`, `tests/test_day3.py`, `config/day3_acceptance.json`, `docs/day3.md`;
sửa top-N thành per-page, giữ fixture runner phù hợp năm task, giữ runtime/pins cũ.

Ngày 4 đã kiểm chứng, accept PASS lúc23:37:23 ngày2026-11-01:

1. [x] Đọc quality_rules.yaml1.1.0 versioned trong job; persist config/hash; thêm quality.check_results và
   quarantine.rejected_pages, migration additive và quyền cần thiết.
2. [x] Phân biệt source/page failure với system failure. Publish valid khi partial;
   all-invalid giữ evidence nhưng fail DAG/không publish.
3. [x] 26unit tests + Airflow/Spark/SQL: valid/empty/script-only/duplicate/HTTP error, input=valid+rejected,
   counts dương, tổng token theo trang/run; kiểm retry/publish không lộ nửa dữ liệu.
4. [x] Web thật partial/dedup, exact replay, system failure2attempts, token/Bronze count/rules drift,
   atomic rollback/visibility PASS; 7services healthy/runtime khớp. Giữ evidence ngày1–3;
   cập nhật README/docs/CONTEXT/PLAN/STATUS; latest tại docs/evidence/day4/latest.json.

Ngày 5 tiếp theo (chưa triển khai):

1. Tách publisher connection/credentials/GRANT khỏi Spark; observer readonly;
   giữ crawler chỉ ghi Bronze và admin chỉ seed/migration.
2. Đồng bộ datasets/schema/owner từ config và lineage Bronze→Spark app→Silver/demo
   theo run_id, transform/rules version/hash và publish status.
3. Test allow/deny thật bằng mỗi role; query lineage một run đã publish.
4. Cập nhật tài liệu/bằng chứng; không đánh dấu monitoring/ngày6 hoặc cold-start/ngày7 xong sớm.
