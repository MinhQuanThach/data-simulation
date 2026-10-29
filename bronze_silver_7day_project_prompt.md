# Prompt khởi tạo dự án: Bronze → Silver trong 7 ngày

## 1. Vai trò và cách làm việc của bạn

Bạn là kỹ sư Data Engineering và người hướng dẫn tôi triển khai một lab nhỏ. Hãy thực sự tạo và sửa các file trong repository, chạy được những kiểm tra mà môi trường cho phép, đồng thời giải thích ngắn gọn mục đích của từng thay đổi.

- Mục tiêu là hiểu tương tác giữa các block bằng một luồng dữ liệu thật, có thể quan sát và chạy lại.
- Triển khai theo từng ngày, mỗi ngày hoàn thành một lát cắt chạy được. Không chỉ đưa hướng dẫn hoặc pseudocode rồi dừng.
- Bắt đầu bằng kiểm tra môi trường, đề xuất phiên bản tương thích, rồi triển khai ngày 1. Không tự coi ngày chưa thực hiện là đã hoàn thành.
- Trước mỗi ngày: nêu mục tiêu, file dự kiến thay đổi và tiêu chí hoàn thành. Cuối ngày: ghi kết quả đã kiểm tra, lệnh chạy lại và việc còn thiếu vào STATUS.md.
- Các lựa chọn nhỏ được tự quyết theo đặc tả. Chỉ hỏi khi thiếu thông tin thực sự chặn triển khai hoặc cần thay đổi phạm vi quan trọng.
- Không tự tạo dữ liệu giả rồi trình bày như dữ liệu crawl thật. Fixture chỉ phục vụ kiểm tra lặp lại.
- Nếu không có Docker daemon hoặc không truy cập được registry/web: vẫn hoàn thiện phần có thể làm, ghi rõ kiểm tra chưa chạy và cung cấp lệnh để tôi chạy tại máy. Không tuyên bố end-to-end đã thành công.

## 2. Mục tiêu, thời lượng và giả định môi trường

- Thời gian: 7 ngày làm việc, dự kiến 3–4 giờ/ngày; đây là ước lượng, không phải lịch chạy tự động.
- Trong hai ngày đầu phải có Data Storage, Orchestration và Execution thực sự kết nối với nhau.
- Đến cuối tuần: truyền một hoặc nhiều URL vào Airflow; crawler lấy HTML thật; lưu Bronze; Spark làm sạch thành Silver và tính word-count; xem được kết quả, metadata, kiểm tra chất lượng và dashboard giám sát.
- Airflow và Spark là bắt buộc. Dùng công cụ miễn phí, chạy local bằng Docker Compose v2.
- Mặc định Linux container, Windows dùng Docker Desktop + WSL2 hoặc Linux/macOS có Docker. Trước khi triển khai, xác nhận OS, CPU architecture, Docker/Compose và tài nguyên thực tế.
- Ngân sách tham khảo: máy 16 GB RAM trở lên, phân bổ khoảng 8 GB cho Docker, 4 CPU và 15–20 GB đĩa trống; điều chỉnh khi kiểm tra thực tế. Không khẳng định đây là mức tối thiểu bảo đảm chạy trên mọi máy.
- Dữ liệu nhỏ: 1–5 trang/lần chạy, độ sâu crawl mặc định 0, tức chỉ lấy nội dung các URL được nhập. Crawl liên kết trong cùng domain là phần tùy chọn sau khi baseline đạt yêu cầu.
- Chọn URL của trang HTML công khai, trả nội dung server-side, không cần tài khoản hoặc JavaScript. Cho phép người dùng đổi URL, không hard-code một website duy nhất.
- Chưa hỗ trợ mọi website: trang chặn bot, yêu cầu đăng nhập, paywall, nội dung dựng bằng JavaScript, PDF và hình ảnh nằm ngoài baseline. Với nguồn không hỗ trợ, báo lỗi rõ ràng.

## 3. Context kiến trúc và phạm vi trách nhiệm

Trong mô hình doanh nghiệp ban đầu:

- Integration tiếp nhận dữ liệu từ nguồn và bàn giao Bronze; trong lab, crawler Python mô phỏng vai trò này.
- Data Model xác định schema, mapping và các quy tắc chuẩn hóa/làm sạch.
- Data Ops xây dựng và vận hành pipeline theo quy tắc đó.
- Data Governance xác định hoặc điều phối chính sách chất lượng, metadata và quyền sử dụng dữ liệu.
- Data Infrastructure cung cấp storage, compute, mạng và các cơ chế truy cập.
- Data Ops, Data Model và Data Governance cùng tham gia xuyên suốt Bronze → Silver.
- BI và Data Scientist là nhóm tiêu thụ downstream, không cần triển khai trong tuần này.

Medallion trong lab:

- Bronze: HTML nguồn cùng URL, thời điểm lấy, HTTP status, content type và hash nội dung; giữ gần nguồn để đối chiếu/tái xử lý.
- Silver: nội dung văn bản đã loại các thành phần HTML không cần thiết, chuẩn hóa Unicode và khoảng trắng; có thông tin liên kết về bản ghi Bronze.
- Word-count là sản phẩm minh họa được tính từ Silver và lưu tại schema demo, không dùng để khẳng định mọi phép tổng hợp thuộc Silver. Nếu mở rộng kiến trúc sau này, các chỉ số phục vụ nghiệp vụ có thể đặt ở Gold.
- Không triển khai Gold, ML hay báo cáo nghiệp vụ trong tuần này.

## 4. Bảy block cần được mô phỏng

| Block                  | Ý nghĩa trong lab                                                   | Công nghệ/chức năng                                        |
| ---------------------- | ------------------------------------------------------------------- | ---------------------------------------------------------- |
| Data Storage           | Lưu Bronze, Silver, staging và quarantine                           | PostgreSQL, persistent Docker volume                       |
| Data Access Enablement | Cấp danh tính và quyền thật cho crawler, job và dashboard           | PostgreSQL roles/GRANT; cấu hình bí mật local không commit |
| Metadata               | Mô tả dataset/schema/owner/rules và lineage của từng lần chạy       | Các bảng metadata trong PostgreSQL; code ghi nhận rõ ràng  |
| Data Quality           | Kiểm tra dữ liệu và quyết định chấp nhận, cách ly hoặc dừng publish | Spark DataFrame/Spark SQL và cấu hình rules có version     |
| Orchestration          | Schedule, dependencies, retry và theo dõi trạng thái task           | Apache Airflow                                             |
| Execution              | Đọc Bronze, làm sạch và ghi dữ liệu; tính word-count                | Apache Spark/PySpark                                       |
| Monitoring             | Ghi metrics và trạng thái; hiển thị lịch sử chạy và dữ liệu lỗi     | PostgreSQL telemetry + Grafana; logs và UI Airflow/Spark   |

Quyết định đơn giản hóa đã được chọn:

- Một công nghệ có thể đảm nhiệm nhiều block; không cần bảy sản phẩm hoặc bảy container riêng.
- PostgreSQL là lựa chọn storage cho lab quan hệ, không giả vờ đây là Delta Lake/Iceberg hay một lakehouse hoàn chỉnh.
- Metadata là catalog/lineage tối giản tự xây, không có đầy đủ discovery, glossary hoặc policy engine của DataHub.
- Data Quality chưa cần thêm dbt, Great Expectations hoặc Soda.
- Monitoring dùng các số liệu ứng dụng được ghi chủ động, chưa cần OpenTelemetry/Prometheus.
- Các phần giản lược phải được chỉ rõ trong README. Không âm thầm mở rộng stack vì muốn giống production.

## 5. Stack Docker và cách liên kết

Các image nguồn ưu tiên:

- Apache Airflow: apache/airflow, custom image để bổ sung Spark client, Java và provider cần thiết.
- Apache Spark: apache/spark, custom image nếu cần dependency Python hoặc JDBC driver.
- PostgreSQL: postgres, dùng hai service tách biệt cho Airflow metadata database và database dữ liệu lab.
- Grafana: grafana/grafana, chọn phiên bản và biến thể miễn phí phù hợp khi triển khai.

Yêu cầu phiên bản:

- Kiểm tra tài liệu chính thức và tag image có thật, hỗ trợ kiến trúc máy, trước khi viết Compose cuối cùng.
- Khóa phiên bản đầy đủ; ghi image digest khi có thể. Không dùng latest trong bản nghiệm thu.
- Khóa cả Airflow provider, Python dependencies và JDBC driver; ghi nguồn và bảng tương thích vào docs/versions.md.
- Xác nhận Airflow, Spark provider, Spark client, master/worker, Java và Python tương thích.
- Cài dependency ở bước build, không pip install hay tải JAR từ internet mỗi lần task chạy.
- Dùng docker compose pull cho image nguồn có thể pull và docker compose build cho các custom image; không giả định image chuẩn chứa sẵn mọi connector.

Topology baseline:

- Airflow dùng LocalExecutor; chạy đủ các service mà phiên bản đã chọn yêu cầu. Không thêm Celery/Redis khi không cần.
- Spark standalone gồm một master và một worker.
- Airflow dùng SparkSubmitOperator, spark-submit client nằm trong container thực thi task.
- PySpark chạy client deploy mode với master URL dạng spark://spark-master:7077; không dùng cluster deploy mode cho PySpark standalone.
- Driver chạy trong container Airflow thực thi task; executor chạy ở Spark worker. Cấu hình địa chỉ driver, bind address và các port để executor gọi ngược được driver qua Docker network.
- Mã Spark và dependency Python phải có sẵn tại những container cần dùng, cùng phiên bản Python client/worker.
- Spark đọc/ghi PostgreSQL qua JDBC; JDBC JAR phải sẵn có trên driver và executor. Phân phối bằng cấu hình Spark hoặc image theo thiết kế rõ ràng.
- Không dùng Docker socket hoặc docker exec từ DAG để thay thế cơ chế submit.
- Không dùng Spark local[*] làm kết quả nghiệm thu cuối; nếu dùng để chẩn đoán phải ghi rõ.
- Chỉ Spark xử lý transformation và word-count; không tính bằng Python rồi chỉ khởi động Spark cho có.
- Crawler và các bước quản lý/publish có thể chạy bằng Python task của Airflow.
- Không truyền HTML/DataFrame hoặc dữ liệu lớn qua XCom; chỉ truyền run_id, tên dataset, thống kê nhỏ.
- Dùng named volume để lưu PostgreSQL; cấp quyền thư mục bind mount đúng cho Airflow/Spark.
- Có healthcheck và kiểm tra sẵn sàng trước khi submit job; depends_on đơn thuần không đủ thay cho healthcheck.
- Không commit mật khẩu; .env.example chứa placeholder. Không in password hoặc JDBC URL chứa secret ra logs.

## 6. Các quan hệ giữa block cần hiện thực

1. Orchestration → Execution: Airflow submit Spark job và nhận trạng thái thành công/thất bại thật.
2. Storage → Execution: Spark đọc các bản ghi Bronze thuộc run_id hiện tại qua JDBC.
3. Execution → Data Quality: áp dụng checks trong job đối với dữ liệu sau làm sạch.
4. Data Quality → Execution/Orchestration: kết quả kiểm tra quyết định quarantine, fail task hoặc publish.
5. Execution → Storage: Spark ghi staging, dữ liệu lỗi và kết quả xử lý; bước publish cập nhật Silver và demo một cách nhất quán.
6. Access → Storage/Execution: dùng đúng database role; test có cả truy cập được phép và truy cập bị từ chối.
7. Metadata → Execution/Quality: code đọc cấu hình schema/rules có version, đăng ký hoặc đồng bộ metadata; không giả vờ catalog tự động sinh logic xử lý.
8. Execution → Metadata: ghi input/output datasets, run_id, transform/rules version và lineage.
9. Orchestration/Execution/Quality → Monitoring: ghi task/run status, thời lượng, số bản ghi và kết quả chất lượng; Grafana query các bảng telemetry bằng tài khoản chỉ đọc.

## 7. Luồng dữ liệu và schema tối thiểu

DAG đề xuất:

validate_config → crawl_to_bronze → spark_clean_validate_count → publish_results → verify_output

Ghi nhận trạng thái failure/success và metrics bằng callback hoặc task có trigger rule phù hợp; final DAG status vẫn phải phản ánh đúng lỗi upstream, không biến DAG lỗi thành success chỉ vì task giám sát chạy xong.

Schema tối thiểu:

- bronze.web_pages: run_id, page_id, requested_url, final_url, fetched_at, http_status, content_type, content_hash, raw_html.
- silver.documents: run_id, page_id, source_url, content_hash, title, clean_text, token_count, processed_at, transform_version.
- demo.word_counts: run_id, page_id, word, count.
- staging: các bảng ứng với đầu ra Spark trước khi được publish.
- quarantine.rejected_pages: run_id, page_id, reason, rejected_at, tham chiếu Bronze; không cần sao chép nguyên HTML nếu không cần.
- metadata.datasets: dataset_name, schema_version, owner, description.
- metadata.pipeline_runs/lineage: run_id, input_dataset, output_dataset, transform_version, rules_version, trạng thái publish.
- quality.check_results: run_id, check_name, status, observed_value, threshold, checked_at.
- telemetry.runs: run_id, start/end, status, duration, input/valid/rejected counts, total_tokens, error summary.

Có thể gộp bảng nếu thiết kế nhỏ hơn vẫn thể hiện rõ trách nhiệm. Ghi nhận lý do và cập nhật tài liệu.

Bronze và crawl:

- Hỗ trợ dag_run.conf hoặc cơ chế tham số tương đương với urls và top_n.
- Dùng HTTP client và HTML parser phổ biến; có timeout, giới hạn kích thước response, redirect và retry có giới hạn cho lỗi tạm thời.
- Giới hạn mặc định 1–5 URL/run, độ sâu 0; tôn trọng robots.txt và hạn chế tốc độ; không bypass chặn truy cập.
- Lưu HTML nguồn đủ để debug và chạy lại transformation không phải crawl lại.
- Báo cáo HTTP status và content type. URL lỗi/không hỗ trợ không được ghi thành tài liệu Silver hợp lệ.
- Bản đầu chọn crawl theo lô. CDC, Kafka, streaming và checkpoint phân tán không thuộc yêu cầu.

Làm sạch và word-count:

- Bỏ script, style và markup; xử lý entity; chuẩn hóa Unicode NFC và khoảng trắng.
- Dùng một quy tắc được ghi rõ để chọn toàn bộ body text hoặc nội dung chính. Không quảng cáo có thể loại mọi navigation/boilerplate trên mọi website.
- Token hóa Unicode, giữ chữ có dấu; lowercase để thống nhất kết quả. Quy định rõ có giữ số không.
- Mặc định không bỏ stop words, không stemming, không lemmatization.
- Với tiếng Việt, đếm token tách theo quy tắc đã công bố, chưa phải phân đoạn từ ngôn ngữ học hoàn chỉnh.
- Tính word-count theo từng trang bằng Spark explode/groupBy/count hoặc tương đương.
- top_n chỉ giới hạn kết quả hiển thị; không được làm mất toàn bộ counts cần đối chiếu tổng token.
- Thứ tự top-N xác định: count giảm dần, word tăng dần khi bằng nhau.

Quality và publish:

- Check có source URL/page_id, HTML có thể parse, clean_text không rỗng, key không trùng và token_count > 0 với trang hợp lệ.
- Theo run phải đối chiếu được input = valid + rejected; counts không âm; tổng demo.word_counts theo trang bằng token_count của silver.documents.
- Bản ghi không đạt được quarantine với reason; nếu không có trang hợp lệ thì không publish và DAG phải thất bại rõ ràng.
- Nếu có một phần trang lỗi, cho phép publish phần hợp lệ nhưng phải ghi trạng thái partial và thống kê rõ. Lỗi hệ thống vẫn phải fail task.
- Spark ghi kết quả vào staging theo run_id, không ghi nửa chừng trực tiếp vào bảng công bố.
- Bước publish dùng một transaction PostgreSQL để đưa các bảng Silver/demo của run từ staging sang trạng thái đã công bố; query demo chỉ nhìn dữ liệu đã publish.
- Retry cùng run_id không tạo dòng trùng. Quy định khóa, cleanup staging và upsert/replace theo run rõ ràng; không mặc định JDBC append là idempotent.
- Một Airflow DAG run mới được xem là snapshot mới có run_id mới; giữ lịch sử là có chủ đích, không coi là trùng dữ liệu do retry.
- Với dữ liệu nhỏ, max_active_runs=1 để đơn giản hóa driver/publish; vẫn phải chống duplicate do task retry.

## 8. Kế hoạch 7 ngày và tiêu chí nghiệm thu

### Ngày 1 — Dựng nền tảng và chứng minh kết nối

- Kiểm tra OS/architecture, Docker/Compose, CPU/RAM/đĩa và network.
- Chọn/pull/build các image; tạo Compose, network, volumes, healthchecks và .env.example.
- Khởi tạo hai PostgreSQL service, Airflow LocalExecutor, Spark master và worker.
- Seed một fixture rất nhỏ vào Bronze.
- Tạo DAG smoke test: Airflow submit Spark đọc bảng Bronze qua JDBC và ghi một kết quả nhỏ vào staging.
- Hoàn thành khi: service healthy; worker đăng ký với master; DAG thành công; có Spark application/executor log thật và dữ liệu đầu ra đọc được bằng SQL.
- Không được coi chỉ mở được UI là đã kết nối ba block.

### Ngày 2 — Hoàn thành lát cắt Bronze → Silver bằng fixture

- Thêm job Spark làm sạch HTML, token hóa và word-count từ fixture.
- Thêm staging/publish tối thiểu và truy vấn top-N.
- Thêm ít nhất check clean_text không rỗng và thử retry cùng run_id.
- Hoàn thành khi: Airflow → Spark standalone → PostgreSQL chạy end-to-end; text Silver và counts khớp fixture; retry không nhân đôi dữ liệu; log nêu rõ app/run_id.
- Đây là mốc bắt buộc có Data Storage, Orchestration, Execution hoạt động cùng nhau.

### Ngày 3 — Thay đầu vào bằng dữ liệu web thật

- Viết crawler và đưa urls vào cấu hình DAG.
- Lưu HTML và metadata HTTP vào Bronze; Spark chạy trên dữ liệu đó.
- Test một URL HTML công khai và đổi sang URL thứ hai mà không sửa source code.
- Hoàn thành khi: có bằng chứng HTML crawl thật, Silver clean_text và top-N; URL lỗi được báo cáo; thay URL bằng cấu hình.

### Ngày 4 — Data Quality và cách ly dữ liệu lỗi

- Tách cấu hình rules có version, bổ sung bảng quality.check_results và quarantine.
- Kiểm tra fixture hợp lệ, nội dung rỗng, HTML chỉ có script/style, bản ghi trùng và HTTP lỗi.
- Chốt policy cho partial success, all-invalid và system failure.
- Hoàn thành khi: trang lỗi không vào Silver; có reason; all-invalid fail và không publish; invariants về số bản ghi/token đúng.

### Ngày 5 — Data Access Enablement và Metadata

- Thêm role crawler chỉ được ghi Bronze; job có quyền đọc Bronze, ghi staging/quarantine và các kết quả được cấp; publisher có quyền công bố đầu ra; observer chỉ đọc metadata/telemetry/demo cần thiết.
- Quản lý kết nối bằng cơ chế local rõ ràng; không để job thường dùng superuser.
- Đăng ký datasets/schema/owner, versions và lineage Bronze → job → Silver/demo theo run_id.
- Hoàn thành khi: chứng minh role được phép làm đúng việc và bị từ chối việc ngoài quyền; truy vấn được input/output lineage của một run cụ thể.
- Seed/migration admin dùng riêng; không dùng để che lỗi phân quyền pipeline.

### Ngày 6 — Monitoring và dashboard

- Thêm Grafana, datasource PostgreSQL chỉ đọc và provisioning dashboard bằng file.
- Hiển thị: run status/duration, input/valid/rejected counts, total_tokens, quality failures và lỗi gần nhất.
- Nối được run_id từ Airflow sang Spark, kết quả SQL và dashboard.
- Hoàn thành khi: một run thành công và một run có lỗi hiển thị khác nhau; restart Grafana vẫn giữ hoặc tái provision được dashboard.
- Alert UI hoặc webhook test local là tùy chọn; không bắt buộc gửi email/Slack.

### Ngày 7 — Tái lập, nghiệm thu và bàn giao

- Chạy quy trình cold start trên volume thử nghiệm riêng; không xóa dữ liệu đang có nếu chưa được yêu cầu.
- Chạy fixture chuẩn, ít nhất hai URL thật, trường hợp lỗi, retry và restart.
- Hoàn thiện README, sơ đồ, data dictionary, troubleshooting và STATUS.
- Ghi rõ những phần đã chạy, chưa chạy và giới hạn của lab.
- Hoàn thành khi đáp ứng checklist mục 10 và có transcript/log/query đủ tái kiểm tra.

## 9. Cấu trúc repository đề xuất

- compose.yaml
- .env.example và .gitignore
- docker/airflow/Dockerfile, docker/spark/Dockerfile
- requirements/ với dependency lock phù hợp
- dags/bronze_to_silver.py
- src/crawler/, src/jobs/, src/common/
- sql/init/, sql/migrations/, sql/queries/
- config/quality_rules.yaml, config/datasets.yaml
- fixtures/ gồm HTML và expected output nhỏ
- grafana/provisioning/, grafana/dashboards/
- scripts/ hoặc Makefile: up, down, trigger, inspect, test
- tests/ cho fixture, invariants, permissions và end-to-end cần thiết
- docs/architecture.md, docs/versions.md, docs/data_dictionary.md
- README.md, CONTEXT.md, PLAN.md, STATUS.md

Không bắt buộc thêm thư viện, framework hoặc nhiều abstraction chỉ để đủ cấu trúc. Giữ module ngắn và liên kết trực tiếp.

## 10. Checklist nghiệm thu cuối tuần

- [ ] Có hướng dẫn chính xác pull/build/start/init/trigger/query/stop bằng Docker Compose.
- [ ] Base image và dependency được khóa phiên bản; không phụ thuộc latest.
- [ ] Airflow điều phối Spark standalone thật và có dấu vết executor xử lý.
- [ ] Trong hai ngày đầu đã có luồng ba block Storage–Orchestration–Execution chạy được.
- [ ] Nhập URL HTML công khai bằng cấu hình và crawl dữ liệu thật.
- [ ] Giữ HTML nguồn tại Bronze; có text đã làm sạch tại Silver.
- [ ] Spark thực hiện tokenization và word-count; có query top-N.
- [ ] Có fixture deterministic để đối chiếu kết quả độc lập với web thay đổi.
- [ ] Có quality results, quarantine và lỗi không bị báo thành success.
- [ ] Retry cùng run không nhân đôi dữ liệu; publish không lộ nửa kết quả.
- [ ] Có database roles và test truy cập bị từ chối.
- [ ] Có metadata, versions và lineage theo run_id.
- [ ] Có dashboard giám sát từ dữ liệu run thật.
- [ ] Restart không làm mất dữ liệu đã lưu trong volume.
- [ ] Có tài liệu giới hạn: static HTML, batch, SQL storage, metadata tự xây, tokenization đơn giản, chưa production.
- [ ] Có STATUS ghi rõ kiểm tra nào đã chạy, kết quả nào quan sát được và phần còn thiếu.

## 11. Context phải giữ cho những session tiếp theo

Luôn cập nhật CONTEXT.md và STATUS.md để session khác không cần đọc toàn bộ hội thoại:

- Mục tiêu và ranh giới Bronze/Silver/demo, lý do chọn PostgreSQL.
- OS/CPU architecture, tài nguyên Docker và các hạn chế mạng đã quan sát.
- Phiên bản image, provider, Java, Python và JDBC driver thực tế.
- Service topology, ports, địa chỉ driver và Spark submit mode.
- Chỉ vị trí cấu hình secrets, tuyệt đối không chép giá trị bí mật vào context.
- Schema, khóa chống trùng, định nghĩa run_id và cơ chế publish.
- Quy tắc làm sạch/tokenization, rules version và transform version.
- Quan hệ bảy block đã triển khai và phần còn giản lược.
- URL đã thử, thời gian thử, run_id và kết quả tóm tắt.
- Ngày đang làm, mục tiêu ngày tiếp theo, blocker và lỗi chưa xử lý.
- Lệnh gần nhất để khởi động, kích hoạt DAG, kiểm tra kết quả và dừng.

## 12. Nguồn chính thức để xác minh khi triển khai

- Airflow Docker Compose: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
- Spark submit provider: https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html
- Spark submit hook: https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/hooks/spark_submit/index.html
- Spark standalone: https://spark.apache.org/docs/latest/spark-standalone.html
- Spark JDBC: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
- Spark Docker source: https://github.com/apache/spark-docker
- PostgreSQL image: https://hub.docker.com/_/postgres
- PostgreSQL privileges: https://www.postgresql.org/docs/current/ddl-priv.html
- Grafana PostgreSQL datasource: https://grafana.com/docs/grafana/latest/datasources/postgres/

Không chọn version chỉ vì trang latest trả về nó. Chọn tổ hợp bản ổn định tương thích và kiểm tra image có thể pull tại thời điểm thực hiện.

## 13. Hành động đầu tiên trong session mới

1. Đọc toàn bộ đặc tả và kiểm tra môi trường hiện có.
2. Nếu chưa biết, hỏi gọn OS/CPU architecture và RAM cấp cho Docker; có thể đồng thời chuẩn bị các file không phụ thuộc câu trả lời.
3. Tóm tắt topology baseline và bảng phiên bản dự kiến, kèm nguồn chính thức.
4. Tạo repository skeleton, CONTEXT/PLAN/STATUS và bắt đầu ngày 1.
5. Chạy smoke test ngày 1 nếu môi trường hỗ trợ. Chỉ báo hoàn thành khi có bằng chứng.
