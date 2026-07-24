# ĐẶC TẢ YÊU CẦU ĐỒ ÁN: LAB 04 - SPARK STREAMING (MÔN NHẬP MÔN DỮ LIỆU LỚN)

**Trường:** Đại học Khoa học Tự nhiên, ĐHQG-HCM[cite: 1].
**Ngôn ngữ lập trình chính:** Python, Apache Spark, cơ sở dữ liệu NoSQL/Graph[cite: 1].
**Mục tiêu cốt lõi:** Xây dựng Đồ thị Thuộc tính Mã (Code Property Graph - CPG) theo hướng tăng dần (incremental), kết hợp luồng dữ liệu thời gian thực (real-time streaming ingestion pipeline)[cite: 1].

---

## 1. YÊU CẦU HỆ THỐNG & KIẾN TRÚC TỔNG QUAN
*   **Phân tích mã nguồn:** Trích xuất các node AST, cạnh luồng điều khiển (CFG), cạnh luồng dữ liệu (DFG), và các cạnh gọi hàm (call edges) từ mã nguồn Python[cite: 1].
*   **Message Broker:** Sử dụng Apache Kafka để quản lý các topic và điều phối thông điệp[cite: 1].
*   **Cơ sở dữ liệu Đồ thị:** Neo4j (lưu trữ Node và Edge)[cite: 1].
*   **Cơ sở dữ liệu Tài liệu:** MongoDB (lưu trữ Metadata)[cite: 1].
*   **Xử lý luồng (Streaming Processing):** Apache Spark Structured Streaming[cite: 1].

---

## 2. CHI TIẾT CÁC TÁC VỤ (TASKS) CẦN THỰC HIỆN

### Task 1: Sao chép Repository và Khám phá File (1 điểm)
*   Thực hiện shallow-clone kho mã nguồn Python được chỉ định từ GitHub để giảm dung lượng tải[cite: 1].
*   Liệt kê toàn bộ các file mã nguồn `.py` trong repository[cite: 1].
*   Được phép (và được khuyến khích) bỏ qua các file test, file setup và các file được tạo tự động[cite: 1].
*   Phải ghi nhận lại tổng số file Python đã khám phá được vào báo cáo[cite: 1].

### Task 2: Dịch vụ phân tích CPG tăng dần (Parser Service) (1.5 điểm)
*   Xây dựng một service bằng Python xử lý từng file `.py` một cách đơn lẻ (không xử lý toàn bộ kho code dưới dạng batch)[cite: 1].
*   Sử dụng một thư viện phân tích CPG/AST tùy chọn (như Joern, tree-sitter, hoặc thư viện `ast` tiêu chuẩn của Python)[cite: 1].
*   Trích xuất 4 thành phần: AST nodes, CFG edges, DFG edges, và call edges[cite: 1].
*   Mỗi thành phần trích xuất phải được phát (emit) dưới dạng một sự kiện (event message) có cấu trúc gửi đến cụm Apache Kafka[cite: 1].
*   Dịch vụ phải hoạt động với giới hạn bộ nhớ (bounded memory)[cite: 1].
*   Phải gán các định danh cố định (stable identifiers) cho từng phần tử để đảm bảo khi xử lý lại không tạo ra dữ liệu trùng lặp ở các bước sau[cite: 1].

### Task 3: Thiết kế Kafka Topic (1.5 điểm)
*   Thiết kế cấu trúc topic trên Apache Kafka để chứa 4 loại sự kiện từ Parser Service[cite: 1].
*   Yêu cầu bắt buộc phải có các topic riêng biệt cho: node events, edge events, source metadata events, và parser error events[cite: 1].
*   Mỗi message gửi đi phải chứa trường phiên bản schema (schema version) phục vụ tính tương thích chuyển tiếp (forward compatibility)[cite: 1].
*   Mỗi message gửi đi phải chứa nhãn thời gian của sự kiện (event time timestamp)[cite: 1].

### Task 4: Đẩy Đồ thị vào Neo4j (2 điểm)
*   Sử dụng **Neo4j Kafka Connector Sink** để kết nối trực tiếp các topic chứa node và edge events vào Neo4j[cite: 1].
*   Dữ liệu cấu trúc đồ thị phải được ghi trực tiếp từ Kafka vào Neo4j mà **không** thông qua lớp trung gian Spark[cite: 1].
*   Logic đẩy dữ liệu (ingestion) phải có tính lũy đẳng (idempotent), đảm bảo việc xử lý lại một node/edge không tạo ra bản sao trùng lặp trong Neo4j[cite: 1].

### Task 5: Đẩy Metadata vào MongoDB (2 điểm)
*   Xây dựng một job bằng **Apache Spark Structured Streaming**[cite: 1].
*   Job này tiêu thụ (consume) các sự kiện metadata từ Kafka và ghi chúng vào MongoDB thông qua **MongoDB Spark Connector**[cite: 1].
*   Job bắt buộc phải thiết lập thư mục kiểm tra (checkpoint location) để có thể tiếp tục xử lý từ offset cuối cùng trong trường hợp khởi động lại[cite: 1].

### Task 6: Kiểm chứng tính lũy đẳng (Idempotent Replay) (1 điểm)
*   Thực hiện chỉnh sửa một file Python trong repository đã clone và cho chạy lại duy nhất file đó qua Parser Service[cite: 1].
*   Phải xác minh được số lượng node và edge trong Neo4j phản ánh đúng bản cập nhật mà không sinh ra node trùng lặp[cite: 1].
*   Phải xác minh được collection trong MongoDB chứa document metadata đã được cập nhật cho file đó[cite: 1].
*   Phải chứng minh được Spark Structured Streaming checkpoint đã bỏ qua chính xác các offset của những file không bị thay đổi[cite: 1].

### Task 7: Sơ đồ kiến trúc (1 điểm)
*   Cung cấp một sơ đồ kiến trúc (Architecture Diagram) mô tả toàn bộ hệ thống luồng dữ liệu trên[cite: 1].

---

## 3. HƯỚNG DẪN NỘP BÀI (SUBMISSION GUIDELINES)
*   **Định dạng nộp bài:** Chỉ nộp duy nhất một URL trỏ tới trang Jupyter Book đã được xuất bản (hosted trên GitHub Pages)[cite: 1]. Không chấp nhận file zip, PDF, hay Word[cite: 1].
*   **Cấu trúc Jupyter Book:**
    *   Được lưu trữ trên một public GitHub repository của nhóm[cite: 1].
    *   Mỗi chương (chapter) tương ứng với một Task[cite: 1].
    *   Phải bao gồm giải thích bằng văn bản về phương pháp đã chọn và lý do[cite: 1].
    *   Phải bao gồm các ô notebook đã thực thi hiển thị kết quả trung gian (số lượng node, mẫu Kafka message, kết quả truy vấn database)[cite: 1].
    *   Phải có ảnh chụp màn hình hoặc hình ảnh nhúng của giao diện UI database[cite: 1].
    *   Mỗi chương kết thúc bằng phần phản ngẫm (những gì hiệu quả, những gì thất bại, cách giải quyết)[cite: 1].
*   **Quản lý Source Code:** Toàn bộ source code phải nằm trong cùng GitHub repository chứa Jupyter Book, được sắp xếp theo cấu trúc thư mục logic[cite: 1].
*   **Commit history:** Các thông điệp commit (commit messages) phải có ý nghĩa và phản ánh tiến độ làm việc của nhóm[cite: 1].
*   **Tài liệu code:** Code phải được ghi chú (comment) rõ ràng, cung cấp đầy đủ hướng dẫn chạy (instructions for running) nếu môi trường phức tạp[cite: 1].