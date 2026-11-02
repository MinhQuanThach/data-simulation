# Ngày 5: quyền truy cập và metadata

Ngày 5 bổ sung bốn identity PostgreSQL, catalog versioned và lineage theo từng
Spark attempt. Kết quả nghiệm thu hiện tại xem [STATUS](../STATUS.md), không suy
ra PASS từ việc có migration hoặc mở được UI. Ngày 6 mới triển khai telemetry/Grafana;
ngày 7 mới nghiệm thu cold start/restart cả tuần.

## Chạy và kiểm tra

Tại PowerShell trong repo, dùng runtime đã build, giữ `.env` và volumes hiện có:

```powershell
docker compose up -d --no-build --wait --wait-timeout 360
python scripts/day5.py migrate
python scripts/day5.py run --case mixed
python scripts/day5.py accept
$day5 = Get-Content docs/evidence/day5/latest.json -Raw | ConvertFrom-Json
python scripts/day5.py inspect --run-id $day5.partial_retry_replay_run
python scripts/day1.py stop
```

Máy mới: `python scripts/init-env.py`, `python scripts/day1.py up`, rồi các lệnh
ngày 5. Không cần dependency/image mới. `up` tái tạo scheduler để nhận credentials
publisher; chỉ `restart` sẽ không nhận thay đổi environment trong Compose.
`migrate` dùng admin trong container `lab-init` chạy ngắn hạn; không dùng admin
cho DAG. Migration và đăng ký catalog chạy lại được, không xóa lịch sử.

## Kết nối và quyền

Secrets là các biến `*_DB_USER`/`*_DB_PASSWORD` trong `.env` local Git ignore;
`.env.example` chỉ placeholder. Init giữ password cũ nếu file tồn tại. Scheduler
nhận crawler/job/publisher, không nhận admin/observer. Executor nhận JDBC job
credentials qua Spark; worker không được cấu hình admin/publisher credentials.
Đây là tách quyền database cho lab, chưa phải cô lập bí mật giữa các Python task
chung container scheduler. Không truyền password qua CLI/XCom hoặc ghi vào evidence.

| Identity mặc định | Đọc được | Ghi được | Bị từ chối tiêu biểu |
|---|---|---|---|
| lab_crawler | Hai bảng Bronze để kiểm snapshot/replay | INSERT Bronze web_pages/fetch_details | UPDATE/DELETE Bronze; staging/Silver/metadata/DDL |
| lab_spark | Bronze, staging, quality/quarantine, catalog/control/lineage, Silver/demo để verify | Staging/quarantine; checks; control metadata và lineage chưa published | Ghi Bronze/Silver/demo, đăng ký catalog, giả published, SET ROLE publisher |
| lab_publisher | Bronze nguồn, staging, quality/quarantine, catalog/control/lineage và output | Silver/demo; chỉ các cột trạng thái publish của metadata/lineage | Ghi Bronze/staging/rules/catalog |
| lab_observer | metadata.datasets, metadata.lineage, metadata.run_summary, demo.published_word_counts | Không | Bronze/HTML, staging, Silver thô, metadata.pipeline_runs, DML/DDL |

Crawler chỉ có quyền **ghi trong Bronze**; SELECT Bronze là quyền đọc cần thiết
để retry/replay không crawl lại. Task crawl còn dùng kết nối control `lab_spark`
riêng cho run lock/config; không cấp quyền metadata cho crawler. Admin chỉ
migration/seed/operator tests. Bốn task identities đều không superuser, CREATE DB,
CREATE ROLE, replication, bypass RLS hoặc membership của role khác. Thu hồi quyền
tạm Spark/publisher dùng chung từ ngày 2; không dùng ALL TABLES để cấp quyền tương lai.

Guard trigger chạy bằng quyền caller, chặn job đặt published/published_at và sửa
metadata đã published. Publisher có UPDATE theo cột; publish Silver/demo/lineage/
run flag cùng transaction. Published retry no-op trước khi ghi. Observer chỉ đọc
view đã lọc public output và metadata cần thiết. Chưa có telemetry table ở ngày 5;
ngày 6 sẽ cấp SELECT view telemetry cụ thể khi tạo bảng.

## Catalog và lineage

`config/datasets.yaml`, schema `1.0.0`, mô tả bốn datasets: Bronze, Silver, demo,
quarantine. Reader kiểm fields, đủ dataset/owner/description, version hỗ trợ.
Catalog không tự phát hiện schema SQL hoặc sinh transformation. `schema_version`
là hợp đồng mô tả chung cho bốn datasets; schema vật lý nằm trong migrations.

Init đăng ký `metadata.datasets`, PK(dataset_name,schema_version), cùng
`metadata.catalog_versions`, PK(catalog_sha256), chứa canonical config JSONB.
Đổi nội dung dataset ở cùng version bị chặn; muốn đổi cần thêm version và reader
hỗ trợ. YAML comments/format/thứ tự datasets không làm đổi canonical SHA-256.
Runtime xác nhận catalog đã đăng ký và pin hash vào `metadata.pipeline_runs`.
Run chưa publish không được đổi catalog/rules giữa retry. Published replay giữ
version/hash cũ; không backfill catalog/lineage giả cho runs ngày 1–4.

`metadata.lineage` có PK(run_id,attempt_id,input_dataset,output_dataset), FK về
run/catalog và dataset schema versions. Mỗi Spark attempt thực sự khởi động có
ba edges `bronze.web_pages → src.jobs.clean_validate_count + app-ID → output`,
với outputs Silver/demo/quarantine. Giữ job_name, app-ID thật, transform/rules
versions/hash, catalog hash, schema versions, counts và timestamps.

Lineage đi running → staged → published; lỗi Spark thành failed và giữ history
attempt. Quarantine có thể đã persist khi all-invalid; lineage failed thể hiện
run không công bố Silver/demo, không có nghĩa diagnostic quarantine đã rollback.
Failure trước khi Spark khởi động không tạo app-ID/lineage giả. Publish kiểm
staged edges, versions/hashes/app/attempt trước khi commit. Query theo run dùng
[day5_lineage.sql](../sql/queries/day5_lineage.sql), join owners/schema versions.
`inspect` chạy chính query này bằng observer qua TCP/password, không dùng admin socket.

## Bằng chứng và xử lý lỗi

`accept` kiểm unit ngày 2–5; migration hai lần; allow/deny thực qua mỗi identity,
SQLSTATE 42501; oracle; partial với Airflow retry; all-invalid/system failure;
web thật HTTP200/404; exact replay fixture/web; transaction rollback/visibility
của Silver/demo/lineage; query observer; runtime driver/worker và bảy service healthy.
Test transaction clone được ghi job_name `test_day2.transaction_snapshot_copy`,
giữ originating Spark app, không trình bày là lần Spark xử lý mới.

Evidence Git ignore ở `docs/evidence/day5`: permissions.json, unit transcripts,
transaction-tests.txt, lineage-psql.txt, runtime/images, từng run có report, SQL,
task/executor logs và observer-lineage.json. `latest.json` chỉ ghi khi đủ PASS
và source hashes không đổi suốt bộ accept. DagRuns failed có chủ đích vẫn đỏ trên UI.

Lỗi quyền: đọc task-logs và permissions.json, chạy `python scripts/day5.py migrate`
để áp đúng grants, rồi `docker compose up -d --no-build --wait --wait-timeout 360`
nếu environment chưa cập nhật. Không dùng admin thay job để vượt lỗi. Catalog
drift: khôi phục config đã đăng ký hoặc thêm version/reader và tạo DagRun mới.
Không reset volumes, sửa ACL hoặc in `.env` để debug.

Nguồn: [PostgreSQL 16 privileges](https://www.postgresql.org/docs/16/ddl-priv.html),
[GRANT theo bảng/cột](https://www.postgresql.org/docs/16/sql-grant.html),
[trigger](https://www.postgresql.org/docs/16/sql-createtrigger.html).
