# Fixture chuẩn bị

Toàn bộ fixture do dự án tự viết, không phải HTML crawl thật. Domain `.invalid`
chỉ là định danh nguồn cho kiểm tra, tuyệt đối không đưa vào crawler web thật.

`expected.json` là oracle viết tay cho ngày 2/4: 12 tokens, 8 từ khác nhau,
top-3 là `chào`, `dữ`, `liệu` (mỗi từ 2 lần). Title không thuộc body text.
Không bỏ stop words; giữ số; dấu câu tách token; tiếng Việt chưa tách từ ngôn ngữ học.

Ngày 1 chỉ seed trang hợp lệ và chứng minh Spark đọc/ghi JDBC; chưa đòi hỏi
thực hiện transformation. Các case lỗi và retry là hợp đồng kiểm tra về sau,
chưa được chạy end-to-end trong bước chuẩn bị.
