# Context chuyển session

## Session ngày 4 hiện tại — đã hoàn thành, PASS (2026-11-01)

Nghiệm thu cuối **23:37:23 Asia/Saigon**, [latest.json](docs/evidence/day4/latest.json).
**Ngày 1–4 PASS; session tiếp theo làm ngày 5. Ngày 5–7 chưa triển khai.**

Yêu cầu hiện tại: đọc toàn bộ đặc tả/Markdown/repo, triển khai ngày4 thực sự,
chỉ báo hoàn thành có bằng chứng, cập nhật CONTEXT/PLAN/STATUS. Chưa yêu cầu commit/push.
Đã đọc tài liệu và code; không thấy AGENTS.md áp dụng; Git ban đầu sạch.
Sandbox exec vẫn lỗi setup refresh; require_escalated được auto-review chấp thuận;
không sửa ACL/settings hoặc reset dữ liệu. Stack đầu session 7/7 healthy,
Docker Linux/x86_64 CPU4/RAM8330498048 bytes. Giữ pins/runtime/volumes/secrets cũ.

Code ngày4: migration004_day4.sql; quality.py/rules1.1.0 reader + canonical SHA256;
quality.check_results PK(run,attempt,scope,check), quarantine.rejected_pages PK(run,page)
FK Bronze; metadata thêm quality_status/valid/rejected/rules config/hash.
Job Spark classify/persist checks, quarantine nguồn lỗi, chỉ valid vào staging;
all-invalid giữ diagnostics nhưng fail; system failure fail, không biến thành trang lỗi.
quality_status=valid/partial/all_invalid/system_failure; publish_status giữ published
cho mọi output đã commit (cả partial), failed cho run lỗi. Demo view không cần đổi.
Publisher kiểm quality gate + SQL coverage/count/token/source/hash/attempt/overlap,
commit Silver/demo/publish flag một transaction. Retry cleanup staging/quarantine
chỉ run chưa publish; check history theo attempt. Published replay no-op.
Fixture mixed/all_invalid/http_error/missing_body/no_tokens/unsupported_type bổ sung;
HTTP fixtures là viết tay, không crawl. Fault flags once/always có mục đích kiểm thử.

Nghiệm thu: scripts/day4.py accept, docs/day4.md; tests policy + DB guards + Spark
standalone fault tại JDBC read để kiểm duplicate vì PK Bronze chặn insert trùng.
Runner kiểm source hashes không đổi trong accept; latest.json chỉ ghi sau đủ PASS.
Kết quả cuối: 26 unit tests Linux; valid fixture12tokens/8words; mixed5=valid1+
rejected4; Airflow up_for_retry thật/publish blocked và exact partial replay giữ nguyên.
All-invalid5=0+5, unsupported type và system failure cả2attempts đều DAGfailed,
downstream upstream_failed/publicrows0. Duplicate Bronze bị PK chặn; duplicate
Spark fault có unique_keys=fail, không publish. Standalone còn kiểm token threshold,
missing source và fetch_error rỗng. Transaction tests chặn counts sai, Bronze count
sai, rules hash drift; reader khác không thấy uncommitted; rollback cả2bảng rồi
retry/no-op PASS. Test dùng đúng SELECT/INSERT/DELETE, không mở thêm quyền staging.
Web thật input3/dedup2=valid1+rejected1, 25tokens; HTTP200/404 cùng body577bytes/hash
nhưng chỉ HTTP200 vào Silver. Source hashes không đổi trong bộ accept cuối.
7/7services healthy; inventory driver/worker/pip/JDBC khớp, imageID vẫn c58044...fdc.
Evidence local docs/evidence/day4 Git ignore; có report/SQL/task/executor logs,
retry/replay, quality-psql.txt, transaction/Spark tests, runtime/images/environment,
web-bronze-output.json và bytes/HTML thật. Chi tiết xem STATUS/day4.md.

Run UUIDs cuối: normal c2b779b4-d32d-521f-95d8-5bfd106a8a34;
partial/retry/replay e339b8cb-4c59-51ed-bec8-55d7fe47c9e1;
all-invalid66be8bcf-f249-5fc2-8e03-d6d22e670a56;
type14e35395-f300-5176-a58b-0a11c5481197;
system8eecf57b-ae0b-5ea3-bf57-8945f3eaea57;
web29135595-89ed-51da-8ed8-cc5e1ef370ae. Spark duplicate87e2a316-65cf-5c66-b56b-42200f6722af,
transactionf8fcecb0-8915-41d5-8e40-c95215c168e8. App IDs và Airflow IDs tại STATUS/handoff-summary.json.
Ngày5 roles publisher/observer + catalog/lineage; ngày6 telemetry/Grafana;
ngày7 cold start/restart/nghiệm thu tuần vẫn chưa làm. Không có blocker thủ công hiện tại.
Ngày5: tách publisher identity/connections/secrets local khỏi job; observer readonly;
allow/deny tests bằng từng role thật; đồng bộ datasets/schema/owner và lineage
Bronze→Spark app→Silver/demo theo run/versions/rules hash. Admin chỉ seed/migration.

Lệnh: python scripts/day4.py migrate; python scripts/day4.py run --case mixed;
python scripts/day4.py accept; python scripts/day4.py inspect --run-id <UUID>.
Khởi động docker compose up -d --no-build --wait --wait-timeout 360;
dừng giữ dữ liệu python scripts/day1.py stop. Chưa commit/push.

## Lịch sử bàn giao ngày 1–3 (không thay trạng thái hiện tại ở trên)

Cập nhật **2026-11-01, Asia/Saigon**. **Ngày 1–3 đã hoàn thành, nghiệm thu PASS.**
Ngày 3 kết thúc lúc **19:51:30**; session tiếp theo là **ngày 4**.
User yêu cầu đọc toàn bộ đặc tả/Markdown/repo, thực sự tạo code/cấu hình ngày 3,
chỉ báo hoàn thành khi có bằng chứng và cập nhật CONTEXT/PLAN/STATUS. Chưa yêu cầu commit/push.

## Ngày 3: context hiện tại

- Đã đọc toàn bộ đặc tả/Markdown/code; working tree ban đầu sạch. Không thấy
  AGENTS.md áp dụng. Terminal sandbox vẫn lỗi setup refresh; require_escalated
  được auto-review chấp thuận. Không sửa ACL/settings hoặc reset dữ liệu.
- Stack đầu session đang dừng, đã up giữ volumes. Docker Linux/x86_64,
  CPU4/RAM8330498048 bytes (~7,76GiB); cuối nghiệm thu 7/7 service healthy.
  Giữ toàn bộ pins ngày1/2; không build lại/thêm dependency/service. Driver/worker
  pip/runtime/JDBC khớp tại docs/evidence/day3/runtime.json và images.json;
  cùng image ID c58044bb8b3c60b8d84ec5b0816515a581a201bdaba8d0f2c8845dfa46ed0fdc.
- Code mới src/crawler/{config,http,pipeline}.py, migration003_day3.sql,
  scripts/day3.py, tests/test_day3.py, config/day3_acceptance.json, docs/day3.md.
  Sửa DAG/init grants, top-N per-page, giữ exact fetch_error trong Spark và cập
  nhật fixture runner kiểm năm task. Working tree hiện có ngày3, chưa commit/push.
- DAG bronze_to_silver hiện: validate_config → crawl_to_bronze →
  spark_clean_validate_count → publish_results → verify_output. Native template
  truyền config dict qua XCom; không HTML/DataFrame. Bỏ urls vẫn chọn fixture.
- Conf web urls list1..5 **trước dedup**, top_n integer1..100 default3. Trim/bỏ
  fragment, giữ path/query/order; run/page IDs theo hợp đồng ngày2. HTTP(S)/host,
  không credentials/control/whitespace nội bộ. Không urls+fixture_case/unknown keys.
- Crawler1.0.0: requests streaming/gzip/deflate, body≤2MiB sau giải nén,
  connect5s/read15s, redirects≤5 kiểm robots từng đích, retry≤2 cho mở request/
  HTTP429/500/502/503/504, backoff1s/2s; rate≥1s/hostname hoặc robots delay/rate.
  Read body lỗi không giữ prefix; task crawl timeout8phút. User-Agent BronzeSilverLab/1.0.
  Không implicit .netrc/proxy auth, giữ TLS verify. Robots404/410 absent, lỗi khác
  conservative; delay/Retry-After>30s hoặc HTTP-date dừng reason riêng, không rút
  ngắn yêu cầu server. REP matcher hỗ trợ groups/longest match/Allow tie/wildcard/$/
  percent paths; urllib.robotparser đọc delay/rate. Cache theo origin trong batch.
- Giữ web_pages 11 cột để tương thích ngày1/2. Bảng mới bronze.fetch_details
  PK/FK(run,page): raw_body bytea sau giải nén, body_bytes, request/robots history,
  selected headers JSONB, crawler_version. Hash bytes trước decode/parse; raw_html
  decode từ HTTP charset hoặc BOM/meta/detect bằng UnicodeDammit. Decode lỗi/NUL
  giữ bytes, không text lỗi. Body đọc dở/quá giới hạn có bytes/html/hash NULL, bytes0.
- metadata.pipeline_runs thêm input_config/requested_count/unique_input_count/
  crawl_complete. Config đã persist khóa theo run. Crawler giữ advisory lock cùng
  Spark/publisher, Bronze+details commit batch atomic. Metadata commit riêng;
  retry phục hồi commit gap bằng kiểm keys/details và không HTTP mới. Replay
  snapshot đã persist không recrawl kể cả lỗi nguồn; muốn nguồn mới tạo DagRun.
- Spark vẫn extraction/token/count trong standalone executor/JDBC. Top-N riêng
  từng trang, counts đầy đủ. Verify web kiểm bytes/hash/HTTP/source/versions/
  số trang/token sums; runner còn đếm độc lập counts từ Silver bằng stdlib host.
  Lỗi SourceError có code an toàn; code/DB error propagate, không báo thành URL lỗi.
- Tests PASS: 16 HTTP/config +5 fixture unit Linux tests; local HTTP/DB chứng minh
  rollback Bronze giữa batch, retry toàn snapshot, replay không HTTP, phục hồi
  commit gap và chặn đổi conf. Local server test tách khỏi public crawl evidence.
- `python scripts/day3.py accept` PASS **2026-11-01 19:51:30 Asia/Saigon**;
  docs/evidence/day3/latest.json. URLs chạy khoảng19:43–19:49, chỉ khác conf:
  example.com HTTP200/577bytes/25tokens; python.org/about/ HTTP200/41265bytes/708tokens.
  Source SHA256 không đổi giữa URLs. Multi input3/dedup2/733tokens/top2 per-page.
  Exact clear/replay web giữ bytes/timestamps/output, cả5 task success/no-op.
  URL404 có Bronze http_404,2Spark attempts fail/downstream upstream_failed,
  public rows0/publisher blocked. Fixture regression năm task cũng PASS.
- UUIDs: first 4e71ca1d-9306-5545-a862-36c577956d09/app-20261101124333-0001;
  second eb33384e-afb9-53f8-8844-17f5393a211f/app-20261101124438-0002;
  multi 283c0f50-e08d-5fc8-9c55-200ce118703a/app-20261101124546-0003;
  invalid a0c17203-dfe1-5884-9e28-62219b9019b6/apps0004→0005;
  fixture 7e1ab0bf-df78-5b29-809b-517c8f7b76a2. Full Airflow IDs/links tại STATUS.
  Targeted fixture retry sau thay DAG cũng PASS: UUID8f0e42fa-2fd1-5eae-8275-7199712f0bd7,
  day2_20261101T124754703121Z: injected fail/up_for_retry/publish blocked/attempt2 success.
- Evidence ngày3: latest.json, unit-day2.txt/unit-day3.txt,
  bronze-transaction-tests.txt, runtime.json/images.json/dag-import-errors.json;
  từng run initial/{report,sql-output}.json, task/executor logs, .body/.html;
  URL1 replay riêng; lỗi publish-blocked.txt. Fixture/retry ở docs/evidence/day2/.
  Local evidence bị Git ignore, có runner/tests để tái tạo; không lưu secrets.
- Thao tác thủ công bắt buộc: **không có**. Stack vẫn chạy, giữ volumes/secrets.
  Ngày3 vẫn fail toàn run nếu có trang lỗi; **ngày4** mới rules reader versioned,
  persisted quality/quarantine, partial/all-invalid và các invariants. Ngày5
  roles/catalog/lineage, ngày6 telemetry/Grafana, ngày7 restart/cold-start chưa làm.

Lệnh hiện tại:

```powershell
Set-Location D:\VCB\data-simulation
docker compose up -d --no-build --wait --wait-timeout 360
python scripts/day3.py migrate
python scripts/day3.py run --urls https://example.com --top-n 5
python scripts/day3.py run --urls https://www.python.org/about/ --top-n 5
python scripts/day3.py accept
$day3 = Get-Content docs/evidence/day3/latest.json -Raw | ConvertFrom-Json
python scripts/day3.py inspect --run-id $day3.first_run --top-n 5
python scripts/day1.py stop
```

## Nền tảng và lịch sử ngày 1–2

## Mục tiêu và môi trường

- Đặc tả: bronze_silver_7day_project_prompt.md. Lab 7 ngày, Airflow + Spark bắt buộc.
- Bronze giữ HTML nguồn → Silver text chuẩn hóa → word-count ở schema demo.
  PostgreSQL là storage quan hệ, metadata tự xây; chưa lakehouse/DataHub/Gold/ML/BI.
- Ngày 1–2 chỉ dùng fixture viết tay. https://fixture.invalid/valid-vi là định danh,
  không crawl domain .invalid. Mục tiêu web: static HTML công khai, 1–5 URL/run,
  batch/depth0, chưa JavaScript/paywall/login/PDF.
- Đã đọc toàn bộ Markdown và code/repo hiện có. Vận hành: docs/day1.md, docs/day2.md.
- Host Windows11 Home10.0.26200 x64, i5-1240P, 16 CPU logic, RAM31.69 GiB.
  Docker Desktop4.75.0, Engine29.5.2, Compose5.1.3, context desktop-linux.
- Đo ngày2: Docker Linux/x86_64, 4 CPU, 8330498048 bytes (~7.76 GiB).
  WSL đã cấu hình 8GB/4 CPU/swap8GB, Docker data E:\Docker\WSL.
  Dung lượng đĩa trong preflight ngày1 là số đo lịch sử, không phải hiện tại.
- Sandbox exec lỗi setup refresh; require_escalated đã được auto-review chấp thuận.
  Không sửa ACL, Git global config hoặc Docker/WSL settings. Không suy ra Docker hỏng.
  Git dùng `git -c safe.directory=D:/VCB/data-simulation ...` theo từng lệnh.
- Giữ nguyên pins ở config/stack.json/Dockerfile/requirements: Airflow3.3.2,
  Spark provider5.5.0, standard/common-compat1.19.0, Spark/PySpark4.0.2,
  Py4J0.10.9.9, Scala2.13.16, Python3.11.16, Temurin Java17.0.17+10,
  PostgreSQL16.15, JDBC42.7.14. Grafana OSS12.4.2 mới pin nguồn, chưa chạy.
- docs/evidence/day2/runtime.json đo lại driver/worker inventory, pip check và hashes,
  cả hai khớp. Image ID thực cả hai container:
  sha256:c58044bb8b3c60b8d84ec5b0816515a581a201bdaba8d0f2c8845dfa46ed0fdc.
  Tag vẫn bronze-silver-runtime:day1; report ngày1 giữ image ID lịch sử riêng.
- Ngày2 không build lại/thêm dependency/service. Shared runtime lấy Spark/JRE từ
  apache/spark, Python từ apache/airflow; worker mang Airflow là giản lược cho lab.
  Build helper ngày1 xử lý PySpark throttling bằng bounded HTTP ranges + SHA-256
  toàn sdist chính thức. JDBC cũng khóa hash tại build, không tải JAR mỗi task.
- `.env` local do init-env.py tạo ngẫu nhiên, chạy lại giữ file; không in/gửi/commit
  secrets hoặc xóa/sinh mới khi DB volume còn password cũ. Auth file precreate ở
  airflow-auth volume, JWT key dùng chung. runtime-freeze-py311.txt là inventory,
  không phải wheel lock để cài lại. Evidence/logs docs/evidence/ bị Git ignore.

## Topology và quyền

- Hai DB riêng airflow-db/lab-db; Airflow LocalExecutor init/api-server/scheduler/
  dag-processor; Spark master/worker; volumes-init/lab-init chạy một lần.
  7 service chạy lâu healthy sau nghiệm thu. Không Celery/Redis/triggerer/Grafana.
- UI loopback: Airflow8080, Spark master8081/worker8082; DB không publish host port.
  Một bridge network, named volumes riêng DB/log/auth/Spark work.
- SparkSubmitOperator client mode → spark://spark-master:7077; driver ở scheduler,
  host airflow-scheduler, bind0.0.0.0, ports39001/39002. Executor thật trên worker.
  Không Docker socket/docker exec trong DAG, không Spark local để nghiệm thu.
- Parallelism1, pool spark_single1, max_active_runs1; worker2 cores/2g,
  executor1 core/1g, driver1g, speculation=false, Spark task.maxFailures=1.
- Admin chỉ init/migration/seed/operator query. Pipeline dùng lab_crawler và lab_spark,
  không superuser/DDL. Ngày2 Spark và Python publisher tạm dùng chung lab_spark;
  tách publisher identity và ma trận allow/deny ở ngày5.
- init-lab.py chạy 001_day1.sql rồi sorted sql/migrations/*.sql; SQL additive/rerunnable,
  cấp quyền cụ thể theo bảng. Không reset volume hoặc truncate lịch sử.

## Schema và code ngày2

- Ngày1: Bronze UUID PK(run_id,page_id), HTTP/source fields; staging.smoke_results
  dùng run_id text chỉ chẩn đoán JDBC. Seed gốc UUID00000000-0000-0000-0000-000000000001,
  fixture425 bytes, SHA256 3cb794bd70768c23cc1a5eb470678df94713fe157e5d860b44277669ff2b64a0.
- run_id = UUIDv5 namespace URL, name compact JSON UTF-8 [dag_id,Airflow run_id].
  Retry giữ UUID, DagRun mới là snapshot mới. page_id SHA256 URL, content_hash hash
  bytes nguồn trước parse. Hợp đồng URL web chi tiết ở docs/data_dictionary.md.
- Migration002_day2.sql thêm staging.documents PK(run text,attempt,page),
  staging.word_counts PK(run text,attempt,page,word) + FK; JDBC ghi UUID string.
  Silver PK(run UUID,page) FK Bronze, demo PK(run UUID,page,word) FK Silver.
- metadata.pipeline_runs tối thiểu: Airflow IDs, versions, active_attempt,
  staging_complete,publish_status,spark_application_id,input_count,total_tokens,
  error_summary,published_at. Chưa catalog/lineage/quality/telemetry đầy đủ.
- DAG bronze_to_silver: prepare_fixture → spark_clean_validate_count → publish_results
  → verify_output. smoke_bronze giữ baseline ngày1.
  Conf: fixture_case valid_vi/empty/script_only; top_n integer1..100 default3;
  test_fail_after_documents_once boolean chỉ thử retry. XCom chỉ UUID/config/stats nhỏ.
- transform.py: BeautifulSoup html.parser, body text; bỏ script/style/noscript/template,
  entities/NFC/collapse whitespace; title lấy riêng. Token lowercase rồi NFC, chuỗi
  chữ Unicode category L hoặc số Nd; giữ số/dấu tiếng Việt, punctuation/underscore split.
  Không stopwords/stemming/lemmatization; chưa tách từ tiếng Việt ngôn ngữ học.
- clean_validate_count.py đọc/ghi JDBC thật, parser/token UDF trên executor;
  counts bằng Spark explode/groupBy/count, giữ toàn bộ counts. Transform/rules1.0.0
  cố định trong code; quality_rules.yaml vẫn là hợp đồng, reader policy ở ngày4.
- Ngày2 fail toàn run nếu missing body/empty/no tokens/HTTP/type lỗi, không publish;
  chưa quarantine hoặc partial. Không che upstream failure bằng final task success.
- Spark và publisher cùng advisory lock theo run; session giữ xuyên commits,
  đóng session nhả lock. Attempt spark-<Airflow try_number> mới dọn staging chỉ run
  chưa publish, counts trước documents, append có PK/FK. Chỉ mark staged sau write+invariants.
- pipeline.py publisher row-lock metadata, kiểm complete attempt, page/token sums,
  source/hash/version rồi một transaction thay Silver+demo+publish flag/time.
  Lỗi rollback cả transaction. demo.published_word_counts view chỉ đọc published runs.
  Published run cùng versions transform/publish no-op, giữ timestamps/application ID gốc.
- Oracle expected.json: text Xin chào, Việt Nam! Xin chào & dữ liệu 2026. Dữ liệu sạch.
  12 tokens, 8 distinct words, top3 chào/dữ/liệu mỗi từ2. Top-N count DESC,
  word ASC COLLATE C; chỉ cắt hiển thị. day2_top_n.sql đã chạy psql thật.

## Bằng chứng và tiếp tục

- python scripts/day2.py accept PASS lúc15:14:41 2026-10-30 Asia/Saigon.
  docs/evidence/day2/latest.json, runtime.json, unit-tests.txt, top-n-psql.txt.
  5 unit tests, Airflow/standalone/SQL oracle, retry, replay, rollback/visibility,
  empty/script-only và 7 service healthy đều PASS. Code/tests/runner tái tạo được evidence.
- Normal: Airflow day2_20261030T080734434942Z, UUID86a330c6-cc88-55a4-ae66-580093de52d2,
  app-20261030080756-0002, cả4 task success try1.
- Retry/replay: Airflow day2_20261030T080857171525Z, UUID24b343ce-5f68-5e28-95e8-dca8b19e3240.
  app-20261030080910-0003 fail sau1 document/0 counts, up_for_retry thật, public rows0,
  publisher bị chặn. app-20261030081015-0004 attempt2 success và cleanup attempt1.
  Clear đúng logical date chạy lại cả4 task: success, Spark try3 no-op, task khác try2;
  toàn bộ snapshot/timestamps/appID giữ nguyên. Có initial/replay/retry-incomplete evidence.
- Transaction test UUID68361ac5-65bc-4f27-b420-08b06089e372: incomplete bị chặn,
  reader khác không thấy uncommitted, lỗi giữa publish rollback cả2 bảng/metadata,
  retry success rồi no-op. transaction-tests.txt trong normal evidence directory.
- Empty UUID5b6be970-c784-58cd-a5fe-dea6d194e57c và script-only
  UUID5d540166-61ed-5dac-b1e4-fb31d26a8e11: DAG failed đúng kỳ vọng, Spark try2,
  downstream upstream_failed, không public rows; reason empty_clean_text.
  Các DagRun đỏ trên UI là test có chủ đích, không phải blocker chưa sửa.
- Ngày1 evidence vẫn docs/evidence/day1/latest.json, runtime.json, supplementary.json.
- Đã nối Storage–Orchestration–Execution + publish/retry thật, basic Access/control metadata.
  Ngày3 đã thêm web; quality/quarantine/partial(ngày4), full permissions/catalog/lineage
  (ngày5), telemetry/Grafana(ngày6), restart/cold-start/nghiệm thu tuần(ngày7).
- Không blocker/thao tác sửa tay bắt buộc ngày2. Stack đang chạy; giữ volumes.
  Tại thời điểm nghiệm thu ngày2, working tree chứa thay đổi ngày2 chưa commit/push;
  ngày3 bắt đầu với working tree sạch. Xem trạng thái hiện tại ở đầu tài liệu.

Lệnh tại D:\VCB\data-simulation:

```powershell
docker compose up -d --no-build --wait --wait-timeout 360
python scripts/day2.py migrate
python scripts/day2.py run
python scripts/day2.py accept
$day2 = Get-Content docs/evidence/day2/latest.json -Raw | ConvertFrom-Json
python scripts/day2.py inspect --run-id $day2.normal_run
python scripts/check-preparation.py
python -m compileall -q src dags scripts tests docker/airflow
git -c safe.directory=D:/VCB/data-simulation diff --check
python scripts/day1.py stop
```

Ngày4 tiếp theo: rules reader/versioned policy, quality.check_results và quarantine,
partial/all-invalid/system failure, kiểm valid/empty/script-only/duplicate/HTTP error,
input=valid+rejected và counts/token invariants. Giữ bằng chứng ngày1–3, không đánh
dấu ngày4–7 xong sớm. Phần lịch sử ngày1–2 bên trên không thay context hiện tại ngày3.
