# Task 5: Source Metadata Ingestion into MongoDB

*(Phần của Tuấn)*
- Xây dựng Spark Structured Streaming kết nối Kafka và MongoDB.
- Schema được ánh xạ sang PySpark StructType.
- Đã cấu hình checkpointLocation thành công.

## Reflection
Việc ánh xạ từ schema.json sang PySpark tốn chút thời gian để chuẩn hóa kiểu dữ liệu. Checkpoint hoạt động tốt, khi tắt mở lại Spark thì dữ liệu được nạp tiếp từ offset bị dừng.
