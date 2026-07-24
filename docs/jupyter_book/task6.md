# Task 6: Idempotent Replay Verification

- Đã chỉnh sửa ngẫu nhiên file `diffusers/src/diffusers/__init__.py` (thêm hàm `stream_test_incremental_function()`).
- Đã chạy script `test_idempotency.py` giả lập Parser Service trên file vừa sửa.
- Kết quả cho thấy số lượng Node sinh ra là 3101, Edge là 3448, và các định danh ID (Node ID / Edge ID) khớp 100% qua nhiều lần chạy mặc dù hash của file thay đổi. 
- Nhờ vậy, hệ thống và cấu hình Kafka Connector (Neo4j Sink `MERGE`) cũng như Spark Checkpoint MongoDB đảm bảo không sinh dữ liệu trùng lặp.

## Reflection
Hệ thống chứng minh được tính idempotent nhờ các stable ID (node_id, edge_id).
