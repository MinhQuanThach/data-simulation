# Trạng thái dự án

Ngày cập nhật: 2026-10-29, Asia/Saigon.

**Hiện tại: chuẩn bị trước ngày 1. Ngày 1–7 chưa thực hiện.**

## Đã thực hiện trong bước chuẩn bị

- Đọc đặc tả và hai ảnh; giữ nguyên phạm vi bảy block, PostgreSQL/Airflow/Spark/Grafana.
- Khởi tạo Git local; repository public tại
  [MinhQuanThach/data-simulation](https://github.com/MinhQuanThach/data-simulation),
  nhánh chính `main`, remote `origin`.
- Tạo README/CONTEXT/PLAN, tài liệu môi trường/phiên bản/kiến trúc/data dictionary,
  .gitignore/.gitattributes/.env.example, cấu hình dự kiến và skeleton.
- Tạo fixture viết tay + expected output độc lập với web: 12 tokens, 8 từ khác nhau.
- Chốt source pins, phương án shared Python runtime, định danh run/page,
  staging/retry/publish, chất lượng và quyền theo vai trò.

## Kết quả đã quan sát

| Kiểm tra | Kết quả | Bằng chứng/giới hạn |
|---|---|---|
| OS/CPU/RAM | Windows 11 Home x64, 16 CPU logic, 31.69 GiB RAM | Read-only CIM ngoài sandbox; chưa đo workload |
| Docker daemon | Hoạt động, Linux/amd64, Desktop 4.75.0, Engine 29.5.2 | docker version/info, context desktop-linux |
| Compose/WSL | Compose plugin 5.1.3; Ubuntu và docker-desktop dùng WSL2 | Ubuntu stopped không ngăn Compose từ PowerShell |
| Tài nguyên Docker | 3.83 GiB, 2 CPU | Cần điều chỉnh ngân sách khởi điểm 8 GiB/4 CPU |
| Đĩa | C 26.65, D 18.97, E 37.56 GiB trống | Chưa biết host drive của disk image Docker |
| Mạng host | Registry 401 challenge; PyPI 200 | Không chứng minh mạng build/container |
| Image nguồn | Bốn tag đã chọn có linux/amd64, digest lưu | Chỉ manifest; chưa pull image layers |
| Dependency | Metadata provider/PySpark và constraints đã đọc | Chưa Linux resolver, pip check, import hoặc build |
| Kiểm tra artifact offline | PASS | File cần thiết, manifest/digest/platform, checksum nguồn, pins, fixture oracle, secret placeholders |
| PowerShell preflight | PASS có cảnh báo tài nguyên | JSON report tại docs/evidence/preflight-2026-10-29.json; bốn manifest checks exit 0 |
| PowerShell syntax | PASS | Parser không báo lỗi |
| Secret/cache Git ignore | PASS | .env/.env.local/.local/logs bị ignore; .env.example không bị ignore |
| Nội dung file mới | PASS whitespace check | Transcript tại docs/evidence/preparation-check.txt; không sửa prompt/ảnh gốc |

Sandbox từ chối CIM/WSL/Docker named pipe/config; read-only ngoài sandbox thành công.
Không coi đây là lỗi cài đặt Docker. Một số lệnh sandbox sau đó gặp setup refresh error;
kiểm tra tiếp bằng read-only ngoài sandbox. Git khác owner giữa CodexSandboxOffline
và DELL; đã dùng safe.directory chỉ cho từng lệnh, không sửa global config/ACL.

## Chưa chạy / chưa triển khai

- Chưa pull/build/custom runtime; Python/Java patch versions chưa quan sát trong container.
- Chưa Compose/Dockerfile/DAG/SQL schema; chưa khởi tạo DB/roles/secrets.
- Chưa Spark application/executor, JDBC output, crawl URL thật hoặc published Silver/demo.
- Chưa quality execution, permission allow/deny, metadata writer, Grafana dashboard.
- Chưa kiểm tra idempotence, transaction rollback, restart persistence/cold start.
- Source pins/constraints không được trình bày như runtime đã xác minh.

## Việc user cần làm

Theo [docs/environment.md](docs/environment.md): điều chỉnh WSL2 8 GiB/4 CPU,
restart Docker theo hướng dẫn sau khi lưu công việc, xác nhận host drive chứa
Docker disk image và dung lượng còn trống. Không cần cài stack trực tiếp vào Windows.
Đã xác nhận `.wslconfig` hiện tại là memory=4GB, processors=2, swap=8GB;
chỉ cần đổi hai khóa đầu, giữ swap/cấu hình khác. Chưa thay đổi settings hệ thống.

Phần chuẩn bị trong repository đã hoàn tất và kiểm tra trong phạm vi nêu trên.
Điều chỉnh tài nguyên Docker và kiểm tra runtime ngày 1 vẫn đang chờ; không có
ngày triển khai nào được đánh dấu hoàn thành từ kết quả kiểm tra artifact.

## Lệnh chạy lại phần chuẩn bị

```powershell
Set-Location D:\VCB\data-simulation
python scripts/check-preparation.py
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/preflight.ps1 -Online -InspectImages
git -c safe.directory=D:/VCB/data-simulation status --short
git -c safe.directory=D:/VCB/data-simulation diff --check
```

Lệnh start/init/trigger/query/stop pipeline chưa có; bổ sung và kiểm tra ở ngày 1.
Mục tiêu tiếp theo: smoke DAG Airflow → Spark standalone → PostgreSQL staging,
nghiệm thu bằng executor log và SQL output, theo [PLAN](PLAN.md).
