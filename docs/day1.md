# Vận hành ngày 1

Phạm vi: fixture viết tay → Bronze PostgreSQL → Spark standalone qua Airflow
→ `staging.smoke_results`. Ngày 1 chưa làm sạch HTML, tính word-count, publish
Silver hoặc crawl web. Trạng thái nghiệm thu xem [STATUS](../STATUS.md).

Chạy tại PowerShell trong `D:\VCB\data-simulation`, Docker Desktop Linux đang bật:

```powershell
python scripts/init-env.py
python scripts/day1.py up
python scripts/day1.py runtime
python scripts/day1.py smoke
python scripts/day1.py status
```

`init-env.py` chỉ tạo `.env` nếu chưa tồn tại; nếu đã có thì giữ nguyên file.
`up` pull hai base runtime theo digest, `docker compose pull` hai PostgreSQL,
build runtime, chạy init/migration/seed và đợi healthchecks. Không cài dependency
vào Windows. Build lần đầu có thể lâu vì tải Spark distribution/PySpark.

Các lệnh Compose tương đương sau khi đã pull/build runtime:

```powershell
docker compose config --quiet
docker compose --progress plain build airflow-scheduler
docker compose up -d --no-build --wait --wait-timeout 360
docker compose exec -T airflow-scheduler airflow dags list-import-errors
docker compose exec -T airflow-scheduler airflow dags unpause smoke_bronze
docker compose exec -T airflow-scheduler airflow dags trigger smoke_bronze
docker compose cp sql/queries/day1_smoke.sql lab-db:/tmp/day1_smoke.sql
docker compose exec -T lab-db psql -X -v ON_ERROR_STOP=1 -U lab_admin -d lab -f /tmp/day1_smoke.sql
```

`day1.py smoke` tạo DagRun mới bằng CLI rồi đợi scheduler chạy thật; không dùng
`airflow dags test` hoặc Spark local mode. Khi PASS, evidence nằm ở
`docs/evidence/day1/day1_<timestamp>/`, con trỏ `latest.json` chỉ được ghi sau khi
kiểm tra DAG/tasks/services, SQL fixture và executor log đều đạt. Các log được lọc
giá trị bí mật từ `.env` trước khi lưu. Run thất bại vẫn lưu report/task logs.

`runtime` lưu inventory của driver/worker và `requirements/runtime-freeze-py311.txt`.
File freeze là inventory phiên bản đã cài trong image, không phải wheel lock có hashes.
Base image digest + constraints + direct pins là đầu vào build; custom image content ID
ghi trong `docs/evidence/day1/runtime.json`.

UI local:

- Airflow: http://localhost:8080 — username/password ở `AIRFLOW_ADMIN_USER` và
  `AIRFLOW_ADMIN_PASSWORD` trong `.env`. Mở file bằng editor local; không gửi secrets.
- Spark master: http://localhost:8081 — worker ALIVE và completed application.
- Spark worker: http://localhost:8082 — executor stdout/stderr.

Airflow dùng SimpleAuthManager cho lab một người, mật khẩu được precreate trong
named volume `airflow-auth` để không sinh/in vào startup log. UI chỉ bind loopback.
Spark dùng cùng custom runtime với Airflow để đồng nhất Python; worker có cả
dependency Airflow là giản lược có chủ đích. Không cần `docker/spark/Dockerfile` riêng.
JDBC JAR sẵn trong `/opt/spark/jars` của cả driver và executor.

Hai DB có volume riêng. `lab-init` dùng admin tạo schema/roles/seed. Task thường
chỉ dùng `lab_crawler` và `lab_spark`, không được cấp superuser/DDL. Ngày 1 crawler
có SELECT Bronze để nhân fixture gốc sang snapshot mỗi DagRun; phân quyền crawler
web và ma trận allow/deny đầy đủ thuộc ngày 5. Seed gốc có run UUID
`00000000-0000-0000-0000-000000000001`, URL `.invalid` khớp fixture oracle.

Bronze có UUID `(run_id,page_id)`; bảng chẩn đoán ngày 1 dùng `run_id text` để JDBC
ghi string trực tiếp. Trước mỗi Spark attempt, xóa riêng staging của run rồi JDBC
append vào bảng có PK. Đây chưa phải cơ chế publish ngày 2. Không xóa lịch sử run khác.
Driver host `airflow-scheduler`, bind `0.0.0.0`, ports nội bộ 39001/39002;
Spark master `spark://spark-master:7077`, deploy mode `client`. Spark pool 1,
Airflow parallelism 1, DAG max_active_runs 1; một executor core/1 GiB cho fixture nhỏ.

Dừng và giữ dữ liệu:

```powershell
python scripts/day1.py stop
```

Không dùng `down -v`, prune hoặc reset Docker để sửa lỗi lab. Nếu khởi động lỗi:

```powershell
docker compose ps -a
docker compose logs --tail 100 lab-init airflow-init
docker compose logs --tail 100 airflow-api-server airflow-dag-processor airflow-scheduler
docker compose logs --tail 100 spark-master spark-worker
```

Init exit 0 là bình thường; bảy service chạy lâu phải healthy. Nếu worker chưa ALIVE,
kiểm tra master/worker log trước submit. Nếu named pipe/access denied chỉ xảy ra
trong sandbox, chạy lại cùng lệnh từ PowerShell của bạn; không mở Docker TCP/đổi ACL.
Nếu login sai sau khi tự đổi password `.env`, chạy lại init có chủ đích:
`docker compose run --rm airflow-init`, rồi `docker compose restart airflow-api-server`.
Thay password DB khi volume đã tồn tại cần `ALTER ROLE` bằng admin, không tự reset volume.

Nguồn thiết kế: [Airflow Docker 3.3.2](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html),
[Airflow config](https://airflow.apache.org/docs/apache-airflow/3.3.2/configurations-ref.html),
[Spark standalone 4.0.2](https://spark.apache.org/docs/4.0.2/spark-standalone.html).
