# Fixture chuẩn bị

Toàn bộ fixture do dự án tự viết, không phải HTML crawl thật. Domain `.invalid`
chỉ là định danh nguồn cho kiểm tra, tuyệt đối không đưa vào crawler web thật.

`expected.json` là oracle viết tay cho ngày 2/4: 12 tokens, 8 từ khác nhau,
top-3 là `chào`, `dữ`, `liệu` (mỗi từ 2 lần). Title không thuộc body text.
Không bỏ stop words; giữ số; dấu câu tách token; tiếng Việt chưa tách từ ngôn ngữ học.

Ngày 1 chỉ seed trang hợp lệ và chứng minh Spark đọc/ghi JDBC. Ngày 2 đã chạy
transform/count theo oracle, task retry sau khi staging ghi dở và replay published
snapshot qua Airflow/Spark standalone/PostgreSQL. Empty/script-only dùng để kiểm
tra chặn text rỗng; kết quả tại STATUS và docs/evidence/day2/. Các case HTTP/type,
duplicate, partial/quarantine có bộ nghiệm thu ngày 4 tại scripts/day4.py.
Ngày 4 thêm missing_body/no_tokens; mixed/all_invalid là batch các fixture đã nêu.
HTTP404/unsupported_type dùng body valid với HTTP fields lỗi có chủ đích.
Xem docs/day4.md và STATUS cho kết quả đã kiểm chứng.
