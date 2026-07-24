# Báo cáo tổng kết - Lab 04 (Nhóm Thi, Tý, Sang, Tuấn)

## Task 1: Repository Cloning and File Discovery
*(Phần này Thi sẽ điền vào)*
- Đã sử dụng lệnh `git clone` để tải mã nguồn `huggingface/diffusers`.
- Số lượng file Python tìm thấy: 1338 files.
- **Reflection**: Quá trình tìm file diễn ra suôn sẻ, đã loại bỏ được các file rác nhờ script.

## Task 2: Incremental CPG Parser Service
*(Phần này Sang sẽ điền vào)*
- Trích xuất AST, CFG, DFG.
- Bắn event lên Kafka.
- **Reflection**: ...

## Task 3: Kafka Topic Design
*(Phần này Tý sẽ điền vào)*
- Kafka topics: `node_events`, `edge_events`, `source_metadata_events`, `parser_error_events`.
- JSON Schemas có `event_time`, `schema_version`.
- **Reflection**: Cấu trúc event đã hoạt động ổn định.

## Task 4: Graph Topology Ingestion into Neo4j
*(Phần này Tý sẽ điền vào)*
- Sử dụng Neo4j Kafka Connector Sink.
- Áp dụng lệnh MERGE của Cypher để đảm bảo tính Idempotent.
- **Reflection**: ...

## Task 5: Source Metadata Ingestion into MongoDB
- Xây dựng Spark Structured Streaming kết nối Kafka và MongoDB.
- Schema được ánh xạ sang PySpark `StructType`.
- Đã cấu hình `checkpointLocation` thành công.
- **Reflection**: Việc ánh xạ từ schema.json sang PySpark tốn chút thời gian để chuẩn hóa kiểu dữ liệu. Checkpoint hoạt động tốt, khi tắt mở lại Spark thì dữ liệu được nạp tiếp từ offset bị dừng.

## Task 6: Idempotent Replay Verification
*(Cả nhóm cùng điền vào sau khi Test)*
- Thay đổi 1 file trong `diffusers`, chạy lại Parser Service.
- Quan sát DB không có dữ liệu trùng lặp.
- **Reflection**: Hệ thống chứng minh được tính idempotent nhờ các stable ID (node_id, edge_id).
