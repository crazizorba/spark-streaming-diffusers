# Báo cáo tổng kết - Lab 04 (Nhóm Thi, Tý, Sang, Tuấn)

## Task 1: Repository Cloning and File Discovery
- Đã sử dụng lệnh `git clone --depth 1 https://github.com/huggingface/diffusers.git` để tải mã nguồn nhanh chóng và tiết kiệm dung lượng.
- Số lượng file Python tìm thấy: 1338 files (sử dụng script `Thi/file_discovery.py`).
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
- Đã chỉnh sửa ngẫu nhiên file `diffusers/src/diffusers/__init__.py` (thêm hàm).
- Đã chạy script `test_idempotency.py` để verify hệ thống sinh ra Node/Edge ID cố định.
- Quan sát log và quá trình Parse, không có dữ liệu trùng lặp (khớp 100% ID dù nội dung file bị đổi).
- **Reflection**: Hệ thống chứng minh được tính idempotent nhờ các stable ID (node_id, edge_id).
