# Trạng thái dự án

## Ngày 5 đã hoàn thành — nghiệm thu PASS

**Ngày 1–5 PASS.** `python scripts/day5.py accept` kết thúc exit0 lúc **11:03:38
ngày 2026-11-02 Asia/Saigon**, [latest.json](docs/evidence/day5/latest.json),
[tóm tắt runs](docs/evidence/day5/handoff-summary.json). Source SHA256 không đổi
trong nghiệm thu và được đối chiếu lại sau đó. Giữ bằng chứng ngày1–4.

Đã đọc toàn bộ đặc tả/Markdown và cấu trúc/code repo; không có AGENTS.md áp dụng.
Tạo migration005_day5.sql, catalog.py, day5.py, test_day5.py, day5_lineage.sql và
[day5.md](docs/day5.md); sửa init grants/Compose/publisher/job/crawler và tests
transaction. Không thêm dependency/service hoặc build image. Admin chỉ init/seed;
task crawler/job/publisher tách kết nối, observer chỉ đọc phần được cấp.

| Kiểm tra | Kết quả thực tế |
|---|---|
| Migration/catalog | Chạy lại hai lần trên volume cũ; 4 datasets/schema1.0.0/owner/description, canonical catalog hash; drift cùng version bị chặn |
| Unit Linux | 31 tests PASS: fixture5, crawler/config16, quality5, catalog5 |
| Quyền thật | 57 probes:13allow/44deny đúng42501; kết nối TCP/password/session_user từng role; không superuser/membership/DDL/temp/SET ROLE admin |
| Crawler/job | Crawler chỉ ghi Bronze và đọc để replay; job đọc Bronze/ghi staging/quarantine/check/control/lineage, không ghi public hoặc giả published |
| Publisher/observer | Publisher riêng ghi Silver/demo và cột publish; observer chỉ metadata cần thiết/demo view; bị chặn raw/staging/DML/DDL |
| Lineage | Bronze → job/app-ID thật → Silver/demo/quarantine theo run/attempt, schema/owner, transform/rules versions/hashes/catalog; failed attempt giữ history |
| Fixture/partial | Normal12tokens/8words; partial5=valid1+rejected4, oracle đúng; retry thật/up_for_retry/publicrows0/publisher blocked rồi attempt2success |
| Replay | Exact-run fixture và web replay đều đủ5tasksuccess; Bronze/output/lineage/checks/timestamps/app IDs không đổi |
| Lỗi | All-invalid5=0+5; system failure2attempts; DAGfailed/downstreamupstream_failed/publicrows0, lineage failed, không báo lỗi thành success |
| Atomic publish | Incomplete/token/Bronze count/rules drift bị chặn; reader không thấy uncommitted; fault rollback Silver/demo/runflag/lineage, retry rồi no-op |
| Web thật | example.comHTTP200 + URL404, input2=valid1+rejected1,25tokens, partial/published; bytes/HTML thật được lưu riêng |
| Runtime | 7/7service healthy; driver/worker inventory/pip/JDBC khớp, imageID giữ c58044...fdc; admin/observer vắng scheduler, credentials DB vắng worker environment |
| Smoke regression | DAG smoke_bronze đủ3task success với crawler/job roles; standalone executor log và JDBC fixture output PASS |
| Kiểm tra cuối | Preparation, compileall, Compose config, diff check PASS; source hashes khớp acceptance; quét179file source/docs/evidence không thấy giá trị secrets local |

Các runs thuộc bộ accept cuối:

| Case | Airflow ID / UUID | Spark applications |
|---|---|---|
| Normal | day5_20261102T035512968050Z / ba461311-4431-5f9a-91c5-a417bd4ab049 | app-20261102035528-0002 |
| Partial/retry/replay | day5_20261102T035600722956Z / 750b5d3a-a308-5e6c-b35a-441b7d79577d | app-20261102035618-0003 → app-20261102035717-0004 |
| All-invalid | day5_20261102T035815525729Z / 6828e63e-5cf5-5c97-8c40-e692391e517f | app-20261102035835-0005 → app-20261102035948-0006 |
| System failure | day5_20261102T040013448216Z / 7f21f478-16ed-547b-96ce-353df32aa590 | app-20261102040035-0007 → app-20261102040130-0008 |
| Web/replay | day5_20261102T040155081651Z / 745516b3-39c8-57a5-9fcb-04abdc94fd8b | app-20261102040216-0009 |

Transaction clone UUID1d1d6a64-06c4-4002-b718-c579c0ea576e dùng originating app,
job_name `test_day2.transaction_snapshot_copy`, không giả một Spark execution mới.
Các run thử trước bộ accept cuối được giữ riêng; một lần accept dừng ở phép thử
DELETE join view (55000), đã sửa test sang bảng demo gốc để kiểm đúng42501.

Bằng chứng: [permissions](docs/evidence/day5/permissions.json),
[transaction](docs/evidence/day5/transaction-tests.txt),
[query observer](docs/evidence/day5/lineage-psql.txt),
[runtime](docs/evidence/day5/runtime.json), [images](docs/evidence/day5/images.json),
[environment](docs/evidence/day5/environment.json),
[web Bronze](docs/evidence/day5/web-bronze-output.json),
[kiểm tra tĩnh](docs/evidence/day5/static-checks.json),
[smoke regression](docs/evidence/day5/smoke-regression.json).
Mỗi run có initial/report.json, sql-output.json, task/executor logs,
observer-lineage.json; partial có retry-incomplete.json và replay/, web có replay/.
Evidence Git ignore; code/tests/runner tái tạo được. Các DAG đỏ là negative tests.
Smoke bổ sung: day1_20261102T040348145702Z,
UUIDb930c5c3-a042-5d3c-a26c-3bed5eff1f33; report/task/executor/SQL tại
docs/evidence/day1/day1_20261102T040348145702Z/. Con trỏ day1/latest.json hiện trỏ
lần regression này; các report ngày1 cũ vẫn giữ nguyên, không dùng làm bằng chứng ngày5.

**Không có blocker/thao tác thủ công bắt buộc.** Stack hiện giữ chạy, volumes và
secrets cũ. Docker Linux/x86_64,4CPU/RAM8330498048bytes. Image thực driver/worker:
`sha256:c58044bb8b3c60b8d84ec5b0816515a581a201bdaba8d0f2c8845dfa46ed0fdc`.
Sandbox exec lỗi setup refresh; require_escalated được auto-review chấp thuận,
không sửa ACL/settings/reset dữ liệu. Chưa commit/push.

```powershell
Set-Location D:\VCB\data-simulation
docker compose up -d --no-build --wait --wait-timeout 360
python scripts/day5.py migrate
python scripts/day5.py run --case mixed
python scripts/day5.py accept
$day5 = Get-Content docs/evidence/day5/latest.json -Raw | ConvertFrom-Json
python scripts/day5.py inspect --run-id $day5.partial_retry_replay_run
python scripts/check-preparation.py
python -m compileall -q src dags scripts tests docker/airflow
git -c safe.directory=D:/VCB/data-simulation diff --check
```

Dừng giữ dữ liệu: `python scripts/day1.py stop`. Session tiếp theo làm **ngày 6**:
telemetry/run/task/duration/count/error, callbacks không che lỗi upstream,
Grafana OSS/datasource observer/dashboard provisioning và kiểm success/error/restart.
**Ngày 6–7 chưa triển khai**; cold start/restart cả tuần thuộc ngày7 dùng project/volume riêng.

## Lịch sử ngày 4 — nghiệm thu PASS

**Ngày 1–4 PASS.** `python scripts/day4.py accept` kết thúc exit0 lúc
**23:37:23 ngày 2026-11-01 Asia/Saigon**. Báo cáo cuối:
[latest.json](docs/evidence/day4/latest.json), [tóm tắt runs](docs/evidence/day4/handoff-summary.json).
Source fingerprints không đổi trong bộ nghiệm thu cuối. Giữ evidence lịch sử ngày1–3;
các lần thử chưa chạy trọn bộ không thay báo cáo cuối này.

Code/config: rules reader1.1.0, migration004_day4.sql, quality.check_results,
quarantine.rejected_pages, Spark classification/checks, SQL publication gate,
fixtures/fault tests và [runner](scripts/day4.py). Transform vẫn1.0.0; không thêm
dependency/service/build. Migration additive chạy lại exit0 trên volume hiện có.
Policy/schema/lệnh chi tiết tại [day4.md](docs/day4.md).

| Kiểm tra | Kết quả quan sát |
|---|---|
| Unit Linux | 26 tests PASS: fixture5, crawler/config16, policy5 |
| Valid fixture | Silver đúng oracle, 12tokens/8words, cả5task success |
| Partial | input5=valid1+rejected4, 12tokens; empty/script-only/HTTP404/missing-body có reason, không vào Silver |
| Retry/replay | Spark attempt1 ghi1document/0counts rồi fail; quan sát up_for_retry/publish refused; attempt2 cleanup và publish; exact clear/replay giữ data/checks/quarantine/timestamps/app ID |
| All-invalid | input5=0+5, gồm no_tokens; 2Spark attempts failed, DAG failed, publish/verify upstream_failed; publicrows0 |
| HTTP/type | HTTP404 và unsupported_content_type cách ly kể cả body có text hợp lệ |
| System failure | Fault sau documents ở cả2attempts; system_failure, DAGfailed/publicrows0; không đổi thành trang lỗi nguồn |
| Duplicate | Bronze PK từ chối insert; Spark JDBC-read fault unique_keys=fail, publisher chặn, publicrows0 |
| Dynamic rules | Reader validate/hash; Spark thực thi min token threshold; missing source và fetch_error rỗng đều reject |
| SQL invariants | Publisher chặn token counts bị sửa sai, Bronze input count sai, rules hash drift; input=valid+rejected, không overlap/attempt cũ, token sums theo trang/run đúng |
| Atomic publish | Reader khác không thấy uncommitted; fault giữa publish rollback Silver/demo/metadata; retry success rồi no-op |
| Web thật | input3/dedup2=1valid+1rejected, 25tokens, status partial/published; HTTP200/404 cùng body577bytes nhưng chỉ200 vào Silver |
| Runtime | 7/7service healthy; driver/worker Python3.11.16/Java17.0.17/Spark4.0.2/provider5.5.0, pip/JDBC inventory khớp |

Các run cuối, đều có report/SQL output/task và executor logs trong
`docs/evidence/day4/<Airflow ID>/initial/`:

| Case | Airflow ID / UUID | Spark application |
|---|---|---|
| Valid | day4_20261101T162528223163Z / c2b779b4-d32d-521f-95d8-5bfd106a8a34 | app-20261101162552-0039 |
| Partial/retry/replay | day4_20261101T162629007765Z / e339b8cb-4c59-51ed-bec8-55d7fe47c9e1 | app-20261101162652-0040 → app-20261101162753-0041 |
| All-invalid | day4_20261101T162852014340Z / 66be8bcf-f249-5fc2-8e03-d6d22e670a56 | app-20261101162908-0042 → app-20261101163004-0043 |
| Type | day4_20261101T163030132776Z / 14e35395-f300-5176-a58b-0a11c5481197 | app-20261101163054-0044 → app-20261101163158-0045 |
| System failure | day4_20261101T163240950409Z / 8eecf57b-ae0b-5ea3-bf57-8945f3eaea57 | app-20261101163304-0046 → app-20261101163430-0047 |
| Web partial | day4_20261101T163555779838Z / 29135595-89ed-51da-8ed8-cc5e1ef370ae | app-20261101163623-0050 |

Standalone policy app-20261101163525-0048; duplicate app-20261101163547-0049,
UUID87e2a316-65cf-5c66-b56b-42200f6722af. Transaction snapshot riêng
f8fcecb0-8915-41d5-8e40-c95215c168e8, không sửa snapshot đã publish của run nguồn.

Bằng chứng bổ sung: [quality psql](docs/evidence/day4/quality-psql.txt),
[transaction](docs/evidence/day4/transaction-tests.txt),
[duplicate/policy Spark](docs/evidence/day4/spark-policy-duplicate-tests.txt),
[Bronze regression](docs/evidence/day4/bronze-transaction-regression.txt),
[runtime](docs/evidence/day4/runtime.json), [images](docs/evidence/day4/images.json),
[environment](docs/evidence/day4/environment.json), [kiểm tra tĩnh](docs/evidence/day4/static-checks.json).
Web thật có [Bronze JSON](docs/evidence/day4/web-bronze-output.json) và .body/.html
theo page SHA256, nguồn example.com và /bronze-silver-day3-missing-page lúc23:35–23:37.
Hai response đều577bytes, SHA25625ddf2c883e0d1958ea971d279a7e4f0fd446724ee3db7db19dadabd4a62e484;
quyết định reject404 dựa HTTP status, không dựa nội dung body.

Quality history giữ từng attempt, quarantine giữ attempt hiện tại. `partial` nằm ở
quality_status; publish_status của mọi output đã commit vẫn published. Không backfill
quality giả cho lịch sử ngày1–3. Spark/publisher tạm chung lab_spark; quyền publisher/
observer và catalog/lineage ngày5, telemetry/Grafana ngày6, cold-start/restart ngày7
**chưa thực hiện**. Các DagRun đỏ ở trên là negative tests có chủ đích.

**Không còn blocker/thao tác thủ công bắt buộc cho ngày4.** Stack vẫn chạy, giữ volumes
và secrets cũ; Docker Linux/x86_64 CPU4/RAM8330498048 bytes. Image driver/worker vẫn
c58044bb8b3c60b8d84ec5b0816515a581a201bdaba8d0f2c8845dfa46ed0fdc.
Sandbox exec lỗi setup refresh, lệnh ngoài sandbox được auto-review chấp thuận;
không sửa ACL/settings, không reset dữ liệu hoặc mở thêm quyền staging. Chưa commit/push.
Evidence local bị Git ignore; code/tests/runner tái tạo được.

```powershell
Set-Location D:\VCB\data-simulation
docker compose up -d --no-build --wait --wait-timeout 360
python scripts/day4.py migrate
python scripts/day4.py run --case mixed
python scripts/day4.py accept
$day4 = Get-Content docs/evidence/day4/latest.json -Raw | ConvertFrom-Json
python scripts/day4.py inspect --run-id $day4.partial_retry_replay_run
python scripts/day3.py inspect --run-id $day4.web_partial_run
python scripts/check-preparation.py
python -m compileall -q src dags scripts tests docker/airflow
git -c safe.directory=D:/VCB/data-simulation diff --check
```

Query quality/token sums theo lệnh tại docs/day4.md; dừng giữ dữ liệu:
`python scripts/day1.py stop`. Session tiếp theo: **ngày5** theo PLAN.

## Lịch sử ngày 1–3

Cập nhật 2026-11-01, Asia/Saigon. **Ngày 1–3 đã hoàn thành, nghiệm thu PASS.
Ngày 4–7 chưa triển khai.** Ngày 3 kết thúc lúc **19:51:30**.
Nghiệm thu ngày 2 lịch sử kết thúc lúc 15:14:41 ngày 2026-10-30.

Đã đọc toàn bộ đặc tả/Markdown/repo; tạo migration, Spark clean/tokenize/count,
transaction publisher, DAG fixture, query top-N, tests và runner ngày 2.
Đó là kết quả ngày 2. Ngày 3 đã có HTML crawl thật như bằng chứng bên dưới.

## Ngày 3: triển khai và bằng chứng

Đã đọc toàn bộ đặc tả/Markdown, cấu trúc/code hiện có; working tree ban đầu sạch.
Không có AGENTS.md áp dụng trong repo/các thư mục cha đã kiểm tra.
Thực sự tạo `src/crawler/{config,http,pipeline}.py`, migration `003_day3.sql`,
runner `scripts/day3.py`, `tests/test_day3.py`, cấu hình URLs nghiệm thu và
[hướng dẫn ngày 3](docs/day3.md). DAG hiện có năm task validate → crawl → Spark
→ publish → verify; giữ fixture regression, Spark standalone và transaction publisher.

| Kiểm tra | Kết quả/bằng chứng ngày 3 |
|---|---|
| HTTP/config tests | 16 tests Linux PASS: URL/dedup, REP/wildcard/redirect, retry/timeout, charset, gzip/decompressed size, robots/rate limit |
| Fixture unit tests | 5 tests ngày 2 PASS trong runtime Linux |
| Bronze transaction | Lỗi sau trang đầu rollback cả batch; retry đủ snapshot; replay không HTTP; phục hồi commit gap và chặn đổi conf PASS |
| URL 1 | `https://example.com`, HTTP 200, 577 bytes, title Example Domain, 25 tokens; top-5 for/is/this=2, a/and=1 |
| URL 2 | `https://www.python.org/about/`, HTTP 200, 41.265 bytes, title About Python™ \| Python.org, 708 tokens; top-5 python45/the20/and16/2026=12/news12 |
| Đổi URL | Hai DagRun tuần tự chỉ khác conf; source hashes không đổi |
| Nhiều trang | 3 URL nhập, dedup còn 2, 733 tokens; top-2 riêng từng trang, toàn counts giữ nguyên |
| Web replay | Clear đúng logical date; cả 5 task success, CRAWL_NOOP/TRANSFORM_NOOP/PUBLISH_NOOP; bytes/timestamps/output giữ nguyên |
| URL lỗi | `/bronze-silver-day3-missing-page`: HTTP 404; Bronze reason http_404; Spark fail sau 2 attempts; downstream upstream_failed; public rows=0; publisher bị chặn |
| Đối chiếu web | Hash bytes, HTTP/source, số trang/token invariants và host count độc lập từ Silver PASS |
| Runtime | Driver/worker inventory/pip check/JDBC checksum khớp; cùng image ID ngày 2, không build/thêm dependency |
| Fixture regression | 5 task success; oracle text/counts/top-3 ngày 2 đúng |
| Spark retry sau thay DAG | Injected fail sau document staging, up_for_retry thật/public rows=0/publisher blocked; attempt2 dọn staging và success, không duplicate |
| Sau nghiệm thu | 7/7 service healthy; không còn blocker hoặc thao tác thủ công bắt buộc |
| Query/tĩnh cuối | psql top-2 trả 4 dòng cho 2 trang; totals25/708; source SHA-256 khớp acceptance, compile/Compose/preparation/imports=[]/diff check PASS |

Run IDs công khai được thử ngày **2026-11-01 khoảng 19:43–19:49 Asia/Saigon**:

| Case | Airflow run / UUID | Application |
|---|---|---|
| URL 1 + replay | day3_20261101T124309353742Z / 4e71ca1d-9306-5545-a862-36c577956d09 | app-20261101124333-0001 |
| URL 2 | day3_20261101T124409707575Z / eb33384e-afb9-53f8-8844-17f5393a211f | app-20261101124438-0002 |
| Multi/dedup | day3_20261101T124517967719Z / 283c0f50-e08d-5fc8-9c55-200ce118703a | app-20261101124546-0003 |
| HTTP lỗi | day3_20261101T124642054686Z / a0c17203-dfe1-5884-9e28-62219b9019b6 | app-20261101124710-0004 → app-20261101124828-0005 |
| Fixture regression | day2_20261101T124904443530Z / 7e1ab0bf-df78-5b29-809b-517c8f7b76a2 | Xem report/executor tại evidence ngày 2 |
| Fixture targeted retry | day2_20261101T124754703121Z / 8f0e42fa-2fd1-5eae-8275-7199712f0bd7 | Xem retry-incomplete/initial tại evidence ngày 2 |

Evidence: [unit tests HTTP](docs/evidence/day3/unit-day3.txt),
[fixture tests](docs/evidence/day3/unit-day2.txt),
[Bronze transaction tests](docs/evidence/day3/bronze-transaction-tests.txt),
[runtime](docs/evidence/day3/runtime.json), [image IDs](docs/evidence/day3/images.json).
[Query psql thật](docs/evidence/day3/top-n-psql.txt) hiển thị top-2 theo trang và
đối chiếu counted_tokens=token_count (25 và 708).
Mỗi run ở `docs/evidence/day3/<Airflow run>/initial/` có report/source SHA-256,
task/executor logs, SQL output, `.body` bytes gốc và `.html` đã decode.
Replay URL 1 lưu tại `replay/`; lỗi có `publish-blocked.txt`.
Lần web thử đầu cũng PASS: UUID 4869f212-f5c7-5f3b-b621-5f9b33379541,
app-20261101124044-0000; không dùng lần thử này thay cho nghiệm thu cuối.

Tổng nghiệm thu: [latest.json](docs/evidence/day3/latest.json) đã ghi **PASS** lúc
19:51:30 ngày 2026-11-01. Targeted fixture retry cũng PASS riêng tại
[report](docs/evidence/day2/day2_20261101T124754703121Z/initial/report.json) và
[staging/publish blocked](docs/evidence/day2/day2_20261101T124754703121Z/retry-incomplete.json).
Fixture regression cuối accept:
[report](docs/evidence/day2/day2_20261101T124904443530Z/initial/report.json).

Crawler 1.0.0: requests/BeautifulSoup/urllib.robotparser sẵn trong image, depth 0,
1–5 URL/run; 2 MiB sau giải nén; connect5s/read15s; redirects5; retry2;
rate ≥1s/hostname hoặc robots delay. Robots trên từng redirect target; lỗi bảo thủ,
404/410 robots coi là không có policy. Delay/Retry-After vượt ngân sách30s dừng rõ
reason, không rút ngắn yêu cầu server. Giữ bytes/hash/encoding/HTTP + audit trong
`bronze.web_pages` và `bronze.fetch_details`. Metadata khóa input_config, requested/
unique counts, crawl_complete. Replay không recrawl; muốn snapshot mới tạo DagRun.

Policy ngày 3 vẫn fail toàn run nếu có trang lỗi; **chưa quarantine, persisted
quality hoặc partial** (ngày 4). Roles publisher/catalog/lineage ngày 5,
telemetry/Grafana ngày 6, cold start/restart/nghiệm thu tuần ngày 7 chưa thực hiện.

Lệnh tái chạy ngày 3:

```powershell
Set-Location D:\VCB\data-simulation
docker compose up -d --no-build --wait --wait-timeout 360
python scripts/day3.py migrate
python scripts/day3.py run --urls https://example.com --top-n 5
python scripts/day3.py run --urls https://www.python.org/about/ --top-n 5
python scripts/day3.py accept
$day3 = Get-Content docs/evidence/day3/latest.json -Raw | ConvertFrom-Json
python scripts/day3.py inspect --run-id $day3.first_run --top-n 5
python scripts/check-preparation.py
python -m compileall -q src dags scripts tests docker/airflow
git -c safe.directory=D:/VCB/data-simulation diff --check
```

Không cần thao tác sửa môi trường thủ công hiện tại. Stack đã dừng trước session,
được khởi động lại giữ volumes; Docker Linux/x86_64, 4 CPU/7,76 GiB.
Sandbox exec vẫn lỗi setup refresh; các lệnh ngoài sandbox được auto-review chấp
thuận. Không sửa ACL/settings hoặc reset dữ liệu. Chưa commit/push.

## Lịch sử ngày 2

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

Sau nghiệm thu ngày 3, session tiếp theo làm **ngày 4** theo [PLAN](PLAN.md):
rules reader versioned, persisted quality/quarantine, partial/all-invalid và invariants.
Không đánh dấu ngày 4–7 hoàn thành bằng kết quả ngày 3.
