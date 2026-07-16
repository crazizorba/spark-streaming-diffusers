# Kế hoạch Thực hiện Lab 04: Spark Streaming (Nhập môn Dữ liệu lớn)
**Repository đã chọn:** [huggingface/diffusers](https://github.com/huggingface/diffusers)

**Mục tiêu:** Xây dựng pipeline xây dựng CPG (Code Property Graph) tăng dần kết hợp với luồng dữ liệu thời gian thực sử dụng Kafka, Neo4j, Spark Streaming và MongoDB.

**Danh sách thành viên:** Thi (Nhóm trưởng), Tý, Sang, Tuấn.

---

## 1. Phân công công việc chi tiết

### 👤 Thi (Nhóm trưởng / System & QA)
**Vai trò:** Thiết lập kiến trúc, quản lý repository, kiểm thử tích hợp và đảm bảo chất lượng.
*   **Thiết kế Kiến trúc (Architecture Diagram):** Vẽ sơ đồ luồng dữ liệu tổng thể (từ Github -> Parser -> Kafka -> Neo4j & Spark/MongoDB).
*   **Task 1: Repository Cloning and File Discovery:**
    *   Tạo Github Repo chung cho nhóm.
    *   Sử dụng lệnh `git clone --depth 1 https://github.com/huggingface/diffusers.git` để tải mã nguồn nhanh chóng và tiết kiệm dung lượng.
    *   Viết script đếm và liệt kê các file `.py` trong repo `diffusers` (loại trừ các thư mục `tests`, `docs`, `setup.py`...), ghi nhận tổng số file.
*   **Task 6: Idempotent Replay Verification (Kiểm thử & Xác minh):**
    *   Sửa đổi 1 file Python ngẫu nhiên trong source code của `diffusers`.
    *   Chạy lại file đó qua Parser Service.
    *   Kiểm tra Neo4j đảm bảo không bị nhân bản (duplicate) node/edge.
    *   Kiểm tra MongoDB đã cập nhật metadata mới nhất cho file đó.
    *   Kiểm tra Spark Checkpoint đã bỏ qua các file không thay đổi.

### 👤 Sang (Data Engineer - Python Core)
**Vai trò:** Xây dựng module Parser trích xuất dữ liệu từ mã nguồn. (Lưu ý: Repo `diffusers` khá lớn, cần chú ý tối ưu code).
*   **Task 2: Incremental CPG Parser Service:**
    *   Nghiên cứu và chọn thư viện (Joern, tree-sitter, hoặc `ast` của Python).
    *   Viết service đọc tuần tự từng file `.py` của thư viện `diffusers`.
    *   Trích xuất 4 thành phần: AST nodes, CFG edges, DFG edges, và call edges.
    *   Tạo ID cố định (stable identifiers) cho mỗi element để đảm bảo tính idempotent.
    *   Đẩy (emit) các structured event messages này vào Kafka.
    *   Đảm bảo service chạy không bị quá tải bộ nhớ (bounded memory) khi xử lý các file lớn.

### 👤 Tý (Data Engineer - Kafka & Graph DB)
**Vai trò:** Quản trị luồng Message Broker và lưu trữ Graph.
*   **Task 3: Kafka Topic Design:**
    *   Thiết lập cụm Apache Kafka.
    *   Tạo các topic riêng biệt: `node_events`, `edge_events`, `source_metadata_events`, `parser_error_events`.
    *   Định nghĩa schema cấu trúc message (phải có trường `schema_version` và `event_time`).
*   **Task 4: Graph Topology Ingestion into Neo4j:**
    *   Cài đặt và cấu hình **Neo4j Kafka Connector Sink**.
    *   Nối (wire) connector vào các topic `node_events` và `edge_events` để ghi thẳng vào Neo4j (không qua Spark).
    *   Cấu hình logic Ingestion đảm bảo tính Idempotent (dùng MERGE trong Cypher hoặc config của Connector để không tạo duplicate).

### 👤 Tuấn (Data Engineer - Spark & Document DB + Documentation)
**Vai trò:** Xử lý luồng Streaming và tổng hợp Báo cáo.
*   **Task 5: Source Metadata Ingestion into MongoDB:**
    *   Viết Apache Spark Structured Streaming job.
    *   Đọc dữ liệu từ Kafka topic `source_metadata_events`.
    *   Sử dụng **MongoDB Spark Connector** để ghi dữ liệu vào collection trong MongoDB.
    *   Cấu hình `checkpointLocation` để Spark có thể tiếp tục chạy từ offset cuối cùng nếu bị gián đoạn.
*   **Báo cáo & Jupyter Book (Nộp bài):**
    *   Khởi tạo Jupyter Book và host trên **GitHub Pages** thông qua public repo của nhóm.
    *   Tạo cấu trúc các chương (chapters) tương ứng với 6 tasks.
    *   Tổng hợp text giải thích, lý luận, hình ảnh giao diện DB (UI views của Neo4j/Mongo), và sample output (từ các thành viên khác) vào Jupyter Book.
    *   Đảm bảo mỗi chapter đều có phần "Reflection" (những gì làm được, lỗi gặp phải và cách giải quyết).

---

## 2. Lộ trình thực hiện (Timeline)

*   **Giai đoạn 1: Khởi tạo & Thiết kế (Thi, Tý)**
    *   Thi: Clone `diffusers`, viết script đếm file. Vẽ Architecture Diagram.
    *   Tý: Dựng môi trường Kafka, tạo Topics.
*   **Giai đoạn 2: Phát triển Core (Sang, Tuấn)**
    *   Sang: Code Parser Service, test trích xuất và bắn event lên Kafka với các file source của `diffusers`.
    *   Tuấn: Dựng môi trường MongoDB, Spark. Code luồng Spark Streaming cơ bản.
*   **Giai đoạn 3: Tích hợp & Lưu trữ (Tý, Tuấn, Thi)**
    *   Tý: Cấu hình Neo4j Kafka Sink để hút data từ Kafka vào Graph.
    *   Tuấn: Hoàn thiện luồng Spark -> MongoDB có checkpoint.
    *   Thi: Chạy thử toàn bộ pipeline với 1 file, sau đó chạy toàn bộ codebase của `diffusers`.
*   **Giai đoạn 4: Kiểm thử Idempotent & Báo cáo (Thi, Tuấn + All)**
    *   Thi: Sửa 1 file bất kỳ trong folder `src/diffusers`, chạy Task 6 để test tính Idempotent. Chụp log và màn hình.
    *   Tuấn: Build Jupyter Book. Các thành viên gửi script, screenshot, notebook cells để Tuấn ghép vào Báo cáo.
    *   Tất cả: Review Jupyter Book, kiểm tra commit message trong Github, xác nhận URL và nộp bài trên Moodle.

---

## 3. Các yêu cầu bắt buộc (Compliance)
*   **Mã nguồn:** Nằm trong Github repository, chia folder logic, commit message có ý nghĩa.
*   **Output:** Chỉ nộp 1 URL duy nhất của Jupyter Book (không nộp file zip, pdf, word).
*   **Idempotent:** Cả Neo4j và MongoDB phải xử lý được việc chạy lại data mà không sinh rác/nhân bản.
