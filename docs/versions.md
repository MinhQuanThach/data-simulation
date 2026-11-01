# Phiên bản đã khóa và kiểm chứng ngày 1

Ngày chọn: 2026-10-29, target linux/amd64. Không chọn version chỉ dựa vào trang latest.
**Đã pull/build và chạy smoke ngày 1 thành công; Grafana mới xác minh nguồn, chưa chạy.**
Inventory: [runtime.json](evidence/day1/runtime.json); dependency đầy đủ:
[runtime-freeze-py311.txt](../requirements/runtime-freeze-py311.txt).
Ngày 2 giữ nguyên pins/dependencies; inventory và image ID **thực sự của container
đang chạy** được đo lại tại [day2/runtime.json](evidence/day2/runtime.json).
Không suy ra image ID hiện tại từ một tag local hoặc từ report lịch sử ngày 1.
Digest dưới đây là manifest riêng linux/amd64, không phải multi-platform index digest.
Nguồn machine-readable: `config/stack.json`, `docs/evidence/manifest-*.json`.

| Thành phần | Pin | Cơ sở chọn |
|---|---|---|
| Airflow | apache/airflow:3.3.2-python3.11 | Release 3.x; Python 3.11; topology API server/scheduler/DAG processor |
| Spark | apache/spark:4.0.2-scala2.13-java17-python3-ubuntu | Spark/PySpark cùng 4.0.2, Scala 2.13, Java 17 |
| Spark provider | apache-airflow-providers-apache-spark==5.5.0 | Airflow >=2.11, PySpark >=3.5.2; dùng full PySpark cho spark-submit |
| Common compat / standard provider | 1.19.0 / 1.19.0 | Theo snapshot constraints Airflow 3.3.2/Python 3.11 |
| Python driver/executor | **3.11.16** từ Airflow image | Đã đối chiếu driver/worker và Python UDF executor; không dùng Python host 3.13 |
| Java | **Temurin 17.0.17+10** từ Spark image đã pin | java -version và spark-submit thực tế trên driver/worker |
| PostgreSQL (hai service) | postgres:16.15-bookworm | Cùng version giảm biến số; minor 16 có release notes chính thức |
| JDBC | org.postgresql:postgresql:42.7.14 | Driver Java 8+; khóa checksum binary trước build |
| Grafana OSS | grafana/grafana:12.4.2 | Image miễn phí, phục vụ PostgreSQL datasource ngày 6 |
| Docker / Compose hiện có | Engine 29.5.2 / plugin 5.1.3 | Giữ CLI đã có, dùng docker compose và cấu hình tương thích Compose v2.14+ |

| Image | linux/amd64 digest |
|---|---|
| Airflow | sha256:4158d6893315cf7c7a81a99f423b7b443dd5454a003f87f4cb0a72748e850978 |
| Spark | sha256:17dcbf3ceb10994198bd32ab8cf2944072ebb865acd00b76c6ece17ed391ffff |
| PostgreSQL | sha256:cf5d353f55240ecc2f811827a15c7d1f2d562f08019b49dca12b5a59f4ab5962 |
| Grafana | sha256:9419d74cb14745b96cf7068c6c5de30851d63ee45bfbe6387d48b12e62146ea4 |

Khi viết Dockerfile/Compose, dùng `tag@sha256:...` đúng platform. Khóa cả base image
và custom image digest khi build được. Tags chỉ đủ tái lập khi digest cũng giữ nguyên.

## Dependency và tính tương thích

Direct pins ở `requirements/airflow.in`; full upstream constraints được lưu local
nguyên vẹn, SHA-256 ở `docs/evidence/sources.json`. Bản project constraints đổi
provider 6.3.2 → 5.5.0 và bổ sung PySpark 4.0.2/Py4J 0.10.9.9 để dùng standalone
spark-submit đầy đủ. Đây là điều chỉnh chủ động, **không phải tổ hợp upstream đã test**.
Các dependency còn lại theo snapshot upstream; không tải lại constraints tại mỗi task.
Linux resolver và `pip check` đã pass; provider import/submit đã chạy thật ngày 1.

Provider 6.x mặc định kéo `pyspark-client` (Connect client) và snapshot upstream
có version client khác Spark đã chọn. Chọn provider 5.5.0 giúp giữ dependency full
`pyspark` rõ ràng và tránh thêm Connect ngoài baseline. Metadata yêu cầu tối thiểu
khớp; build/import/task thực tế đã được chứng minh bằng smoke ngày 1.

Spark Ubuntu image cài Python bằng apt của Ubuntu Jammy; không giả định là 3.11.
Thiết kế shared runtime: dùng Airflow Python 3.11 làm base, multi-stage lấy Java 17
và Spark distribution từ apache/spark đã pin. Cùng custom runtime cho scheduler,
master, worker. `pyspark==4.0.2` là Python package đầy đủ; `SPARK_HOME=/opt/spark`
phải trỏ đúng distribution 4.0.2, không trỏ một distribution khác từ PATH.
Dockerfile ngày 1 kiểm tra đường dẫn binary/JAR, `pip check` và tương thích thư viện
JRE với base Debian; build và runtime ngày 1 đã pass các kiểm tra này.

JDBC tải ở bước chuẩn bị/build, version/checksum lưu trong evidence; đặt JAR trên
driver và executor, không tải từ internet mỗi lần chạy. Chưa cần thêm provider PostgreSQL:
Python tasks dùng psycopg2 trực tiếp, Spark dùng JDBC.

Đã tải cache JDBC 42.7.14 (1,221,004 bytes), SHA-256:
`73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c`.
Đây là hash bytes tải qua HTTPS, chưa kiểm chứng chữ ký độc lập; Dockerfile
ngày 1 đã kiểm tra đúng hash này trên driver và worker. Cache `.local/vendor/` không commit.

## Cổng kiểm tra ngày 1

Build ngày 2026-10-30 gặp throttling single-stream PySpark (mẫu 10 MiB trong container
timeout 30s, chỉ nhận 2.50 MB, ~83 KB/s). `docker/airflow/fetch_pyspark.py` tải đúng
sdist chính thức theo ranges 1 MiB, tối đa 8 luồng, mỗi range có timeout/retry và
kiểm Content-Range/size; toàn file 434209940 bytes phải khớp SHA-256
`938b4a1883383374d331ebfcb5d92debfa1891cf3d7a6d730520a1a2d23f1a90` trước pip install.
Metadata/hash từ `https://pypi.org/pypi/pyspark/4.0.2/json`. Không đổi dependency
version, không dùng mirror, không bỏ TLS/checksum, không tải dependency lúc task chạy.

- Pull các nguồn theo digest, build custom runtime, resolve dependency trong Linux/Python 3.11.
- `pip check`, import SparkSubmitOperator, `spark-submit --version`, `java -version`,
  `python --version` trên driver và worker; export full dependency lock/inventory.
- Xác nhận Python minor trùng; Spark client/master/worker trùng 4.0.2; JDBC cùng JAR/hash.
- Smoke DAG thực sự có executor log và JDBC output; digest có thật chưa chứng minh runtime.
- Nếu resolver hoặc runtime không tương thích: sửa pins có giải thích, cập nhật evidence,
  không tự nâng version bằng pip hoặc đổi Spark local mode để báo pass.
- Trước bật Grafana ngày 6, kiểm tra lại patch trong dòng đã chọn; nếu cần đổi thì
  ghi rõ lý do, pin mới và xác minh manifest. Không dùng floating tags.

## Nguồn chính thức

- [Airflow release](https://airflow.apache.org/announcements/)
- [Airflow Docker 3.3.2](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html)
- [Constraints 3.3.2 / Python 3.11](https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt)
- [Spark provider 5.5.0 requirements](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/5.5.0/index.html)
- [Spark 4.0.2 requirements](https://spark.apache.org/docs/4.0.2/index.html)
- [Spark Docker source](https://github.com/apache/spark-docker/tree/master/4.0.2)
- [PostgreSQL 16.15](https://www.postgresql.org/docs/16/release-16-15.html)
- [PostgreSQL official image](https://hub.docker.com/_/postgres)
- [pgJDBC download](https://jdbc.postgresql.org/download/)
- [Grafana Docker OSS](https://grafana.com/docs/grafana/latest/setup-grafana/configure-docker/)
