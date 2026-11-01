# Trạng thái dự án

Cập nhật 2026-10-30, Asia/Saigon. **Ngày 1 và ngày 2 đã hoàn thành, nghiệm thu PASS.
Ngày 3–7 chưa triển khai.** Nghiệm thu ngày 2 kết thúc lúc **15:14:41**.

Đã đọc toàn bộ đặc tả/Markdown/repo; tạo migration, Spark clean/tokenize/count,
transaction publisher, DAG fixture, query top-N, tests và runner ngày 2.
Đầu vào là fixture viết tay; **chưa crawl web thật**.

## Kết quả kiểm tra ngày 2

| Kiểm tra | Kết quả/bằng chứng |
|---|---|
| Runtime | Driver/worker cùng Python 3.11.16, Java 17.0.17, Spark 4.0.2, provider 5.5.0; pip check/JDBC hash PASS |
| Migration | Additive/rerunnable chạy nhiều lần trên volume hiện có, exit 0; không reset DB |
| Unit tests | 5 tests Linux PASS: oracle tiếng Việt, empty/script-only, Unicode, entities/excluded elements, thiếu body |
| DAG end-to-end | Normal run đủ 4 task success try 1; có Spark standalone application/executor Running task thật |
| Silver | title/clean_text/hash/source/page_id khớp expected.json và bytes fixture |
| Word-count | Đủ 8 từ, tổng 12 tokens; top-3 chào/dữ/liệu mỗi từ 2; count DESC/word COLLATE C ASC |
| Retry sau ghi dở | Attempt 1 fail sau 1 document/0 counts; Airflow up_for_retry thật; publisher từ chối; public rows 0 |
| Retry thành công | Attempt 2 cleanup staging cũ riêng run; output đúng oracle, không duplicate/attempt cũ |
| Replay cùng run | Clear đúng logical date, 4 task lại success; transform/publish no-op; snapshot/timestamps/app ID giữ nguyên |
| Transaction publish | Reader khác không thấy uncommitted; lỗi giữa publish rollback cả 2 bảng/metadata; retry success rồi no-op |
| Empty/script-only | Hai DAG failed đúng kỳ vọng sau 2 Spark attempts; reason empty_clean_text; downstream upstream_failed; không publish |
| SQL query | day2_top_n.sql chạy psql thật: top-3 đúng, counted_tokens=token_count=12 |
| Sau nghiệm thu | 7/7 service healthy; worker registered; Docker Linux/x86_64 4 CPU/~7.76 GiB |
| Kiểm tra tĩnh | Compose config, compile Python, offline preparation, DAG imports=[] và git diff --check PASS |
| Secrets | Không thấy giá trị secrets local trong source/docs/evidence đã quét; không lưu .env/password/token |

Tổng hợp: [latest.json](docs/evidence/day2/latest.json),
[runtime.json](docs/evidence/day2/runtime.json), [unit tests](docs/evidence/day2/unit-tests.txt),
[psql transcript](docs/evidence/day2/top-n-psql.txt). Evidence/logs local bị Git ignore;
code/tests/runner đã có để tái tạo trên máy khác.

## Run IDs và bằng chứng

| Case | Airflow run / UUID | Kết quả |
|---|---|---|
| Normal | day2_20261030T080734434942Z / 86a330c6-cc88-55a4-ae66-580093de52d2 | success; app-20261030080756-0002 |
| Retry + replay | day2_20261030T080857171525Z / 24b343ce-5f68-5e28-95e8-dca8b19e3240 | app-20261030080910-0003 fail chủ đích → app-20261030081015-0004 success; replay no-op |
| Empty | day2_20261030T081119560084Z / 5b6be970-c784-58cd-a5fe-dea6d194e57c | failed đúng kỳ vọng, không publish |
| Script-only | day2_20261030T081250111038Z / 5d540166-61ed-5dac-b1e4-fb31d26a8e11 | failed đúng kỳ vọng, không publish |

- Normal: [report](docs/evidence/day2/day2_20261030T080734434942Z/initial/report.json),
  [SQL](docs/evidence/day2/day2_20261030T080734434942Z/initial/sql-output.json),
  [task logs](docs/evidence/day2/day2_20261030T080734434942Z/initial/task-logs.txt),
  [executor](docs/evidence/day2/day2_20261030T080734434942Z/initial/executor-app-20261030080756-0002.txt).
- Retry: [incomplete/up_for_retry](docs/evidence/day2/day2_20261030T080857171525Z/retry-incomplete.json),
  [attempt 2](docs/evidence/day2/day2_20261030T080857171525Z/initial/report.json),
  [replay](docs/evidence/day2/day2_20261030T080857171525Z/replay/report.json).
  Spark try 2 sau retry, try 3 sau clear; task khác try 2 sau clear.
- [Transaction tests](docs/evidence/day2/day2_20261030T080734434942Z/transaction-tests.txt):
  snapshot riêng 68361ac5-65bc-4f27-b420-08b06089e372, không sửa normal snapshot đã publish.
- Invalid: [empty](docs/evidence/day2/day2_20261030T081119560084Z/initial/report.json),
  [script-only](docs/evidence/day2/day2_20261030T081250111038Z/initial/report.json).
- Run thử đầu cũng PASS: day2_20261030T080622399731Z / 264d2ff8-7026-5cd1-b87e-20b6bea81c7d,
  app-20261030080636-0001.

## Implementation và giới hạn

002_day2.sql thêm staging.documents/staging.word_counts, Silver/demo và control metadata.
Staging theo run+attempt, JDBC UUID string; public tables UUID và FK về Bronze/Silver.
Chỉ mark staged sau write+invariants. Publisher dùng advisory lock + metadata row lock,
một transaction thay Silver/demo/publish flag. Run đã publish cùng versions là no-op.

Transform/rules 1.0.0: body text, bỏ script/style/noscript/template, entities/NFC/
whitespace; token lowercase chữ Unicode L và số Nd; không stopwords/stemming,
chưa phân đoạn từ tiếng Việt. UDF trên executor, counts bằng Spark explode/groupBy/count.
Demo view chỉ đọc published, top-N không cắt counts đầy đủ.

Ngày 2 fail toàn run nếu có trang lỗi; **chưa quarantine/persisted quality/partial**
(ngày 4). YAML hiện là hợp đồng, chưa reader policy. Metadata chỉ điều khiển
attempt/publish, chưa catalog/lineage (ngày 5). Spark và publisher tạm dùng chung
lab_spark; tách publisher/test deny ở ngày 5. Chưa telemetry/Grafana (ngày 6),
restart/cold-start/nghiệm thu cả tuần (ngày 7).

Hai DagRun failed trên UI là test invalid có chủ đích, không đổi thành success.
Snapshot mới được giữ là lịch sử, không phải duplicate do retry.

## Blocker và môi trường

**Không còn blocker/thao tác thủ công bắt buộc cho ngày 2.** Stack vẫn chạy.
Sandbox exec lỗi khởi tạo; require_escalated được auto-review chấp thuận;
không sửa ACL/settings hệ thống. Không có action bị auto-review từ chối còn tồn đọng.

Giữ pins/runtime ngày 1, không thêm dependency/build/service. Image ID thực của
container driver/worker đo ngày 2:
sha256:c58044bb8b3c60b8d84ec5b0816515a581a201bdaba8d0f2c8845dfa46ed0fdc;
tag local bronze-silver-runtime:day1. Report ngày 1 giữ digest lịch sử riêng.
Secrets ở .env local/airflow-auth volume, không chép vào tài liệu/evidence/Git.

## Lệnh chạy lại và bàn giao

```powershell
Set-Location D:\VCB\data-simulation
docker compose up -d --no-build --wait --wait-timeout 360
python scripts/day2.py migrate
python scripts/day2.py run
python scripts/day2.py accept
$day2 = Get-Content docs/evidence/day2/latest.json -Raw | ConvertFrom-Json
python scripts/day2.py inspect --run-id $day2.normal_run
docker compose cp sql/queries/day2_top_n.sql lab-db:/tmp/day2_top_n.sql
docker compose exec -T lab-db psql -X -v ON_ERROR_STOP=1 -U lab_admin -d lab -v "run_id=$($day2.normal_run)" -v top_n=3 -f /tmp/day2_top_n.sql
python scripts/check-preparation.py
python -m compileall -q src dags scripts tests docker/airflow
git -c safe.directory=D:/VCB/data-simulation diff --check
```

Máy mới chưa runtime: python scripts/init-env.py rồi python scripts/day1.py up
trước lệnh ngày 2. Xem [docs/day2.md](docs/day2.md). UI Airflow http://localhost:8080,
Spark http://localhost:8081 / http://localhost:8082. Xem tài khoản trong editor local,
không gửi .env. Dừng giữ dữ liệu: python scripts/day1.py stop; không down -v/prune/reset.

Ngày 1 đã PASS: [latest](docs/evidence/day1/latest.json), [runtime](docs/evidence/day1/runtime.json),
[auth/roles](docs/evidence/day1/supplementary.json), [preflight](docs/evidence/preflight-day1-2026-10-30.json),
vận hành [day1.md](docs/day1.md).

Session tiếp theo **ngày 3** theo [PLAN](PLAN.md): crawler configurable urls/top_n,
robots/timeout/size/redirect/rate limit, Bronze HTTP/source metadata thật; thử hai URL
bằng cấu hình, lưu HTML/Silver/top-N evidence. Chưa tự triển khai ngày 3.
