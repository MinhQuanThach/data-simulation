# Chuẩn bị môi trường Windows và Docker

Kiểm tra ngày 2026-10-29 theo Asia/Saigon. Đây là số liệu quan sát, chưa chạy workload lab.

| Mục | Kết quả |
|---|---|
| OS | Windows 11 Home 10.0.26200, x64 |
| CPU/RAM host | Intel i5-1240P, 16 CPU logic, RAM 31.69 GiB; free RAM lúc kiểm tra 16.62 GiB |
| Docker Desktop / Engine | 4.75.0 / 29.5.2; daemon Linux/amd64, context desktop-linux |
| Compose | Plugin 5.1.3, lệnh `docker compose`; chưa cần cài hoặc downgrade |
| WSL | Default version 2; Ubuntu WSL2 stopped, docker-desktop WSL2 running |
| Docker nhận | **3.83 GiB RAM, 2 CPU** |
| .wslconfig hiện có | `[wsl2]`: memory=4GB, processors=2, swap=8GB |
| Đĩa trống | C: 26.65 GiB, D: 18.97 GiB, E: 37.56 GiB |
| Docker disk usage | Build cache 17.66 GB reclaimable; chưa dọn/xóa gì; đây không phải dung lượng trống |
| Mạng host | Docker registry 401 auth challenge bình thường; PyPI 200; manifest image đọc được |
| Cổng UI dự kiến | Không thấy listener 8080/8081/8082/3000 tại lúc kiểm tra; sẽ kiểm tra lại ngày 1 |
| Python/Git host | Python 3.13.12, Git 2.45.0.windows.1; host Python chỉ dùng check offline |

Sandbox ban đầu bị từ chối CIM, WSL, Docker config/named pipe. Các kiểm tra chỉ đọc
ngoài sandbox thành công. Không cần sửa ACL, mở Docker TCP, cài lại Docker hoặc chạy
mọi thứ bằng Administrator chỉ để khắc phục lỗi quyền của sandbox.

## Việc bạn cần làm trước ngày 1

### 1. Tăng tài nguyên WSL cho Docker

Máy hiện có đủ RAM host để thử ngân sách **8 GiB RAM, 4 CPU**, như đặc tả đề xuất.
Đây là ngân sách khởi điểm cần đo khi chạy, không bảo đảm mọi workload.
Với WSL2, tài nguyên do `.wslconfig` quản lý, có thể không có thanh kéo RAM/CPU
trong Docker Desktop như Hyper-V.

1. Lưu công việc trong các terminal/app WSL và dừng các workload Docker/WSL khác
   trước khi restart: `wsl --shutdown` sẽ dừng tất cả distro WSL2.
2. Mở PowerShell Windows của bạn. Xác định file:

   ```powershell
   Join-Path $env:USERPROFILE '.wslconfig'
   notepad (Join-Path $env:USERPROFILE '.wslconfig')
   ```

3. Nếu file đã có, sao lưu trước bằng Explorer và **sửa/ghép các khóa**, giữ các
   cấu hình khác. Không chép đè toàn bộ file. Nếu chưa có, tạo file đúng tên
   `.wslconfig`, không thành `.wslconfig.txt`. Trong section `[wsl2]` đặt:

   ```ini
   [wsl2]
   memory=8GB
   processors=4
   swap=8GB
   ```

   Máy bạn đã có file với memory=4GB, processors=2, swap=8GB. Chỉ cần sửa
   memory và processors như trên; giữ swap=8GB và các cấu hình khác đang có.
4. Quit Docker Desktop qua biểu tượng ở system tray. Sau đó chạy:

   ```powershell
   wsl --shutdown
   ```

5. Mở lại Docker Desktop từ Start menu, đợi Engine running. Trong Settings → General,
   xác nhận dùng WSL 2 based engine và Linux containers.
6. Chạy:

   ```powershell
   docker context show
   docker version
   docker info --format '{{.OSType}} {{.Architecture}} CPU={{.NCPU}} RAM_bytes={{.MemTotal}}'
   docker compose version
   wsl --list --verbose
   ```

   Kỳ vọng daemon Linux/x86_64, CPU ít nhất 4 và RAM xấp xỉ 8 GiB (có thể thấp hơn
   một ít do overhead). Nếu Docker vẫn nhận 4 GiB/2 CPU, kiểm tra phần mở rộng tên
   file, section `[wsl2]`, và đã Quit Docker Desktop + shutdown WSL trước mở lại chưa.
   Nếu context sai, dùng `docker context use desktop-linux` trong terminal này.

Không cần mở Ubuntu để chạy Compose từ PowerShell. Chỉ khi muốn chạy lệnh trong
Ubuntu: bật Docker Desktop → Settings → Resources → WSL Integration → Ubuntu,
Apply, rồi kiểm tra `docker version` bên trong Ubuntu.

### 2. Xác định ổ chứa dữ liệu Docker và kiểm tra đĩa

Workspace ở D: không có nghĩa images/volumes ở D:. `/var/lib/docker` là đường dẫn
trong Linux VM, không cho biết ổ Windows. Kiểm tra Docker Desktop → Settings →
Resources/Advanced (nếu phiên bản có mục Disk image location); nếu không thấy,
dùng hướng dẫn [Docker WSL data location](https://docs.docker.com/desktop/features/wsl/).
Ghi lại host drive chứa disk image và dung lượng trống của ổ đó.

Ngân sách đặc tả là thêm khoảng 15–20 GB trống cho lab, gồm images/build cache/volumes;
cần theo dõi sau build. D: còn 18.97 GiB, gần khoảng tham khảo; E: còn 37.56 GiB.
**Chưa cần tự chuyển Docker disk image hoặc xóa cache để chuẩn bị.** Nếu thiếu đĩa,
chọn việc dọn/chuyển cụ thể sau khi biết disk location và dữ liệu cần giữ.
Không dùng `docker system prune`, `docker builder prune`, `docker compose down -v`
hoặc Factory reset trong bước này.

### 3. Kiểm tra lại và cung cấp kết quả

Trong PowerShell bình thường của bạn:

```powershell
Set-Location D:\VCB\data-simulation
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/preflight.ps1 -Online -InspectImages
python scripts/check-preparation.py
```

`ExecutionPolicy Bypass` chỉ áp dụng tiến trình PowerShell mới, không thay policy hệ thống.
Script preflight không sửa cấu hình và không pull image layers. Thông tin cần gửi:
Docker RAM/CPU mới, context/daemon Linux, host drive chứa Docker disk image, lỗi nếu có.
Không gửi `.env`, password, token hay `.docker/config.json`.

## Git và ownership của sandbox

Git repository vừa khởi tạo thuộc tài khoản CodexSandboxOffline. Khi chạy Git
bằng tài khoản Windows DELL, Git có thể báo `detected dubious ownership`.
Với đúng workspace này, dùng ngoại lệ chỉ cho từng lệnh:

```powershell
git -c safe.directory=D:/VCB/data-simulation status --short
git -c safe.directory=D:/VCB/data-simulation diff --check
```

Chưa sửa cấu hình Git global hoặc ACL. Không cần áp dụng ngoại lệ `*` cho mọi repo.
Đây là khác biệt tài khoản giữa sandbox và terminal Windows, không phải lỗi Docker.

## Tech stack: chưa cần cài thêm vào Windows

Không cài Airflow, Spark, Java, PostgreSQL hoặc Grafana trực tiếp lên Windows,
không sửa Python/Conda host. Các thành phần chạy trong Linux containers;
dependency/JDBC được cài khi build ngày 1. Docker/WSL/Git đã tồn tại.
Chưa cần tài khoản cloud, Kubernetes hoặc Docker Hub login để chạy baseline công khai;
nếu gặp rate limit registry sẽ xử lý theo lỗi thực tế, không bỏ kiểm tra TLS.

Nguồn chính thức: [Docker resources trên WSL2](https://docs.docker.com/desktop/settings-and-maintenance/settings/#resources),
[Microsoft .wslconfig](https://learn.microsoft.com/en-us/windows/wsl/wsl-config),
[Airflow Docker prerequisites](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html).
