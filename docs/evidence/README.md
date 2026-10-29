# Evidence chuẩn bị

- `preflight-2026-10-29.json`: số liệu máy và Docker từ script chỉ đọc, ngoài sandbox.
- `preflight-recheck-2026-10-29.json`: kiểm tra sau khi user tăng tài nguyên,
  Docker 7.76 GiB/4 CPU, không warnings, bốn image digest inspect thành công.
- `docker-storage-recheck-2026-10-29.json`: đường dẫn E:\Docker\WSL do user xác nhận,
  kiểm tra thư mục/VHDX, dung lượng ổ E: và ba khóa tài nguyên WSL.
- `manifest-*.json`: raw multi-platform manifest trả về từ Docker Hub cho tag đã chọn.
  `config/stack.json` giữ digest platform linux/amd64 bên trong manifest đó.
- `package-metadata.json`: metadata PyPI chính thức của Airflow/provider/PySpark;
  gồm requirements và hashes artifacts phát hành, không phải lock đã resolve.
- `sources.json`: nguồn và SHA-256 các snapshot đã tải.
- `jdbc.json`: version, URL, SHA-256 bytes tải qua HTTPS. Binary cache trong
  `.local/vendor/` bị Git ignore; checksum này chưa là kiểm chứng chữ ký độc lập.
- `preparation-check.txt`: kết quả kiểm tra offline, parse PowerShell và Git hygiene.

Tái kiểm tra môi trường/manifest bằng:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/preflight.ps1 -Online -InspectImages
python scripts/check-preparation.py
```

Không có chứng cứ image layers đã pull/build, container đã chạy, DAG thành công
hoặc dữ liệu web thật. Không thêm secrets vào evidence. Tên file ghi ngày khảo sát;
timestamp UTC nằm trong report, diễn giải thời gian theo Asia/Saigon khi trao đổi.
