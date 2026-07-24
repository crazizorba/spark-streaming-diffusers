# Nhật ký công việc - Tý (Data Engineer - Kafka & Graph DB)

Dưới đây là danh sách chi tiết các công việc, mã nguồn và cấu hình hạ tầng tôi đã xử lý để giải quyết **Task 3 (Kafka Topic Design)** và **Task 4 (Graph Topology Ingestion into Neo4j)**.

## Ngày 20/07/2026

### 📝 Các file đã THÊM MỚI / CHỈNH SỬA:
1. **`Ty/docker-compose.yml`**: 
   - *Mục đích*: Xây dựng toàn bộ hạ tầng Data Engineering cục bộ. Bao gồm: `cpg_zookeeper`, `cpg_kafka` (Message Broker trung tâm), `cpg_kafka_ui` (Giao diện giám sát port 8888), `cpg_neo4j` (Graph DB port 7474), và `cpg_kafka_connect` (Dịch vụ Sink tích hợp sẵn plugin Neo4j).
   - *Cách xử lý*: Cấu hình chi tiết các biến môi trường, tối ưu RAM, tự động thiết lập Replication Factor = 1 cho môi trường local.
   - *Tình trạng*: Chạy thành công.

2. **`Ty/schemas/*.schema.json`** (Gồm 4 file: `node_event`, `edge_event`, `source_metadata_event`, `parser_error_event`): 
   - *Mục đích*: Định nghĩa cấu trúc JSON chuẩn (Schema) cho các Message đẩy lên Kafka. 
   - *Cách xử lý*: Đảm bảo bắt buộc phải có trường `schema_version`, `event_time`, và các trường định danh `node_id`, `edge_id` (Stable identifiers) để làm cơ sở cho cơ chế chống trùng lặp (Idempotent).
   - *Tình trạng*: Hoàn thành.

3. **`Ty/neo4j_sink_node.json`**: 
   - *Mục đích*: File cấu hình REST API cho Kafka Connect lắng nghe topic `node_events`.
   - *Cách xử lý*: Sử dụng câu lệnh Cypher `MERGE (n:CodeNode {node_id: event.node_id}) SET n.type = event.node_label, n += event.properties`. Lệnh `MERGE` giải quyết triệt để tính Idempotent: nếu ID đã có thì chỉ cập nhật, chưa có mới tạo.
   - *Tình trạng*: Hoàn thành.

4. **`Ty/neo4j_sink_edge.json`**: 
   - *Mục đích*: File cấu hình REST API cho Kafka Connect lắng nghe topic `edge_events`.
   - *Cách xử lý*: Dùng Cypher `MATCH` tìm 2 đầu Node, sau đó dùng `MERGE` để tạo Cạnh (CALLS/INHERITS) giữa 2 Node đó để đảm bảo Idempotent.
   - *Tình trạng*: Hoàn thành.

### 🛠️ Các lệnh đã chạy (Terminal Commands):
- `docker-compose up -d`: Khởi chạy và liên kết toàn bộ 6 containers của cụm Kafka + Neo4j.
- `curl -X POST -H "Content-Type: application/json" -d "@neo4j_sink_node.json" http://localhost:8083/connectors`: Bắn cấu hình cấu hình lên Kafka Connect thông qua REST API.
- Tương tự với lệnh `curl` cho file `neo4j_sink_edge.json`.

### 📌 Kế hoạch tiếp theo (Next Steps):
- Phần hạ tầng luồng Message và Graph DB (Task 3 & 4) đã đi vào hoạt động 100%. 
- Chờ **Sang (Python Core)** hoàn thiện Parser Service (Task 2) để tự động bắn dữ liệu JSON lên Kafka.
- Hỗ trợ **Tuấn (Spark & MongoDB)** trong việc consume data từ topic `source_metadata_events`.
- Cùng **Thi (System & QA)** test kiểm chứng Idempotent ở bước nghiệm thu cuối cùng.
