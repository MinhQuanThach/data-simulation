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

Ngày 1 có Dockerfile thực tế ở `docker/airflow/Dockerfile`. Helper build tải sdist
PySpark chính thức theo HTTP ranges và xác minh SHA-256 toàn file để xử lý
throttling stream lớn; pip cài file gốc đó, version vẫn bị ràng buộc bởi
`pyspark==4.0.2`. Không dùng `--no-deps` hoặc bỏ qua resolver.
Sau build, `python scripts/day1.py runtime` chạy `pip check` trên driver và worker,
xuất `runtime-freeze-py311.txt` cùng inventory thực tế vào `docs/evidence/day1/runtime.json`.
Freeze giữ direct artifact file URI/hash do pip ghi nhận; file tạm được xóa sau
build nên freeze này là inventory, không dùng như requirements cài lại. Đầu vào
build là airflow.in + constraints + helper source URL/hash + base digest.
Xem STATUS để biết các bước nào đã thực sự pass.

Lệnh cài dependency trong Dockerfile ngày 1 (có thêm URL/hash PySpark nêu trên):

```bash
python -m pip install --constraint /opt/lab/requirements/constraints-project-py311.txt --requirement /opt/lab/requirements/airflow.in
python -m pip check
```
