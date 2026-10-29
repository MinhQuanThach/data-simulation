# Dependency preparation

`airflow.in` khóa các dependency trực tiếp. `constraints-upstream-py311.txt`
là snapshot constraints chính thức Airflow 3.3.2/Python 3.11, giữ nguyên.
`constraints-project-py311.txt` là bản dự án: đổi Spark provider sang 5.5.0,
thêm `pyspark==4.0.2`, `py4j==0.10.9.9`; mọi thay đổi ghi rõ ở đầu file.

Lý do: provider 5.5.0 yêu cầu bản PySpark đầy đủ, phù hợp `spark-submit`.
Provider 6.x chuyển dependency mặc định sang `pyspark-client`; không đưa thêm
Spark Connect hoặc một Spark client khác phiên bản vào baseline.
Pin còn lại theo snapshot upstream. Xem metadata và SHA-256 tại `docs/evidence/`.

Đây là đầu vào build đã pin, chưa phải bằng chứng resolver/build thành công.
Ngày 1 phải resolve trong Linux/Python 3.11, chạy `pip check`, import provider,
so sánh Spark/Python driver-worker, xuất lock đầy đủ kèm hashes hoặc inventory
từ image đã build và ghi digest custom image. Không cài stack vào Python host.

Lệnh ở bước build (Dockerfile sẽ được viết ngày 1):

```bash
python -m pip install --constraint /opt/lab/requirements/constraints-project-py311.txt --requirement /opt/lab/requirements/airflow.in
python -m pip check
```
