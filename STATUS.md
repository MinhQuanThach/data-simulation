# Trạng thái dự án

Cập nhật 2026-10-30, Asia/Saigon. **Ngày 1 hoàn thành, nghiệm thu PASS. Ngày 2–7 chưa triển khai.**

Đã đọc toàn bộ đặc tả, Markdown và cấu trúc repo; thực sự tạo Compose, Dockerfile,
SQL khởi tạo, DAG/job JDBC smoke và scripts vận hành. Ba block Storage → Execution
← Orchestration đã chạy cùng nhau bằng fixture viết tay; chưa có dữ liệu crawl web.

## Kết quả kiểm tra thực tế

| Kiểm tra | Kết quả/bằng chứng |
|---|---|
| Môi trường | Windows 11 x64; Docker Linux/amd64 4 CPU, 7.76 GiB; preflight không warnings |
| Pull/build | Ba base image đã pull đúng digest; shared runtime build thành công |
| Dependency | Linux resolver và pip check PASS; 446 dòng inventory tại requirements/runtime-freeze-py311.txt |
| Driver/worker | Python 3.11.16, Temurin Java 17.0.17+10, Spark/PySpark 4.0.2, Scala 2.13.16 |
| Provider/JDBC | Spark provider 5.5.0 import/submit PASS; JDBC 42.7.14 đúng SHA-256 trên driver/worker |
| Init | volumes-init, lab-init, airflow-init đều exit 0; migration và fixture seed thật |
| Services | 7/7 healthy; Spark worker ALIVE, đăng ký với master |
| DAG | Hai DagRun smoke_bronze success; mỗi run cả 3 task success, try_number=1 |
| Execution | Có application ID và executor stderr chứa Running task; Python UDF chạy trên worker |
| JDBC/SQL | Mỗi run đúng 1 staging row, html_bytes=425, hash khớp fixture gốc |
| UI/auth | Đăng nhập bằng secrets local và authenticated DAG API đều HTTP 200; không lưu token |
| Roles | lab_crawler/lab_spark không superuser, không CREATEDB/CREATEROLE; task không dùng admin |
| Kiểm tra tĩnh | Compose config, compile Python, offline preparation check, DAG import errors=0 |

Evidence:

- [Preflight ngày 1](docs/evidence/preflight-day1-2026-10-30.json).
- [Runtime và custom image digest](docs/evidence/day1/runtime.json),
  [auth/roles/PostgreSQL](docs/evidence/day1/supplementary.json).
- Run đầu: Airflow `day1_20261030T024406869425Z`, UUID `625dcd74-33d0-5a6b-9795-7f9d9d905d8f`,
  Spark `app-20261030024425-0000`; report tại docs/evidence/day1/day1_20261030T024406869425Z/.
- Run cuối: Airflow `day1_20261030T024720408784Z`, UUID `ff1a7cb4-6fb7-5712-ba50-cfe99bae83fd`,
  Spark `app-20261030024732-0001`; chạy khoảng 09:47 Asia/Saigon.
  [Report](docs/evidence/day1/day1_20261030T024720408784Z/report.json),
  [SQL](docs/evidence/day1/day1_20261030T024720408784Z/sql-output.json),
  [Airflow task log](docs/evidence/day1/day1_20261030T024720408784Z/task-logs.txt),
  [executor log](docs/evidence/day1/day1_20261030T024720408784Z/executor-logs.txt),
  [master state](docs/evidence/day1/day1_20261030T024720408784Z/spark-master.json).
- [latest.json](docs/evidence/day1/latest.json) chỉ được ghi khi toàn bộ smoke checks PASS.

Fixture URL `https://fixture.invalid/valid-vi` là định danh dữ liệu viết tay, không được crawl.
SHA-256 HTML: `3cb794bd70768c23cc1a5eb470678df94713fe157e5d860b44277669ff2b64a0`.
Hai DagRun là hai snapshot mới; không dùng chúng để tuyên bố đã kiểm tra retry cùng run.

## Lỗi đã xử lý và giới hạn

Sandbox không tạo được exec process (setup refresh error); chạy các lệnh cần thiết ngoài
sandbox đã được auto-review chấp thuận. Không sửa ACL, Git global config hoặc Docker/WSL settings.
Git khác owner được xử lý bằng safe.directory chỉ cho từng lệnh.

Tải single-stream PySpark bị throttling (~83 KB/s trên mẫu 10 MiB, timeout sau 30s).
Đã xử lý tại build bằng HTTP ranges 1 MiB, tối đa 8 luồng, giới hạn retry/timeout,
kiểm Content-Range/size và SHA-256 toàn bộ sdist chính thức. File 434209940 bytes đã
khớp hash; pip resolver/build/pip check đều pass. Không cần thao tác mạng thủ công.

**Không còn blocker ngày 1 hoặc thao tác sửa môi trường thủ công bắt buộc.**
Stack vẫn đang chạy để có thể mở UI và kiểm tra tiếp. Secrets ở `.env` local và
named volume airflow-auth; không ghi giá trị bí mật vào tài liệu/evidence/Git.

Chưa triển khai/kiểm tra: clean text, tokenization/word-count/Silver publish (ngày 2),
crawl hai URL thật (ngày 3), quality/quarantine đầy đủ (ngày 4), ma trận quyền allow/deny
và metadata/lineage (ngày 5), telemetry/Grafana (ngày 6), retry/rollback/persistence/cold start
nghiệm thu toàn tuần (ngày 7). Ngày 1 mới có cleanup staging riêng run trước Spark attempt;
chưa tuyên bố cơ chế publish nghiệp vụ hoặc idempotence đã nghiệm thu.

## Lệnh tiếp tục và vận hành

```powershell
Set-Location D:\VCB\data-simulation
python scripts/init-env.py
python scripts/day1.py up
python scripts/day1.py runtime
python scripts/day1.py smoke
python scripts/day1.py status
docker compose exec -T airflow-scheduler airflow dags list-import-errors
docker compose cp sql/queries/day1_smoke.sql lab-db:/tmp/day1_smoke.sql
docker compose exec -T lab-db psql -X -v ON_ERROR_STOP=1 -U lab_admin -d lab -f /tmp/day1_smoke.sql
python scripts/check-preparation.py
python -m compileall -q src dags scripts docker/airflow
git -c safe.directory=D:/VCB/data-simulation diff --check
```

UI: Airflow http://localhost:8080, Spark master http://localhost:8081,
worker http://localhost:8082. Tên đăng nhập/mật khẩu Airflow ở AIRFLOW_ADMIN_USER /
AIRFLOW_ADMIN_PASSWORD trong `.env`; mở bằng editor local, không gửi file.
Dừng và giữ volumes: `python scripts/day1.py stop`. Không `down -v` hoặc prune.
Chi tiết init/healthchecks/troubleshooting: [docs/day1.md](docs/day1.md).

Session tiếp theo bắt đầu ngày 2 theo [PLAN](PLAN.md): Spark clean/tokenize/count fixture,
staging nghiệp vụ + transaction publish, kiểm tra output theo fixtures/expected.json
và retry cùng run_id không nhân đôi. Chưa tự chuyển sang ngày 2 trong session này.
