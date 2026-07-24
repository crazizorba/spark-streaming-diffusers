# Nhật ký công việc - Tuấn (Data Engineer - Spark & Document DB + Documentation)

## Ngày 23/07/2026

### 📝 Các file đã THÊM MỚI / CHỈNH SỬA:
1. **`Ty/docker-compose.yml`**: 
   - *Mục đích*: Bổ sung dịch vụ MongoDB (`mongo:6.0`) vào hạ tầng chung để chuẩn bị cho phần Metadata Ingestion.
   - *Tình trạng*: Hoàn thành.

2. **`Tuan/mongo_ingestion.py`**:
   - *Mục đích*: Viết script Apache Spark Structured Streaming đọc dữ liệu Metadata từ Kafka topic `source_metadata_events` và lưu vào MongoDB (collection `source_metadata`).
   - *Cách xử lý*: Mapping schema từ JSON sang `StructType` của PySpark. Cấu hình `checkpointLocation` tại `./spark_checkpoints/source_metadata` để đảm bảo tính chịu lỗi và tránh trùng lặp.
   - *Tình trạng*: Hoàn thành code (Chờ môi trường chạy lên để test).

3. **`Tuan/report_draft.md`**:
   - *Mục đích*: Viết sơ bộ khung sườn Báo cáo cho Jupyter Book, phân loại sẵn 6 Tasks và phần Reflection theo form chuẩn để cả nhóm có thể cùng vào điền.
   - *Tình trạng*: Hoàn thành.

### 📌 Kế hoạch tiếp theo (Next Steps):
- Build file `report_draft.md` lên thành Jupyter Book chuẩn sau khi Sang code xong Parser.
- Kiểm thử tích hợp (Integration Test) phần Spark kết hợp cùng lúc với Kafka và Mongo để chắc chắn Checkpoint hoạt động mượt mà.
