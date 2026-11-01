# Context chuyển session

Cập nhật 2026-10-30, Asia/Saigon. **Ngày 1 và ngày 2 đã hoàn thành có evidence PASS;
bước tiếp theo là ngày 3.** Ngày 2 nghiệm thu lúc 15:14:41.
User yêu cầu đọc toàn bộ đặc tả/Markdown/repo, thực sự tạo code/cấu hình ngày 2,
chỉ báo hoàn thành khi có bằng chứng và cập nhật CONTEXT/PLAN/STATUS. Chưa yêu cầu commit/push.

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
  Chưa crawl web(ngày3), quality/quarantine/partial(ngày4), full permissions/catalog/lineage
  (ngày5), telemetry/Grafana(ngày6), restart/cold-start/nghiệm thu tuần(ngày7).
- Không blocker/thao tác sửa tay bắt buộc ngày2. Stack đang chạy; giữ volumes.
  Working tree chứa thay đổi ngày2, chưa commit/push.

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

Ngày3 tiếp theo: validate urls/top_n, HTTP crawler có robots/timeout/size/redirect/
retry/rate limit, Bronze giữ cả lỗi nguồn; Spark xử lý input web, verify theo
invariants thay oracle khi chạy web. Thử hai URL thật bằng cấu hình, lưu HTTP/HTML/
Silver/top-N evidence. Không dùng fixture giả làm crawl hoặc đánh dấu ngày4–7 xong sớm.
