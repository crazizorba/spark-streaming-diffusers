# Đồ án: Lab 04 - Spark Streaming (Xây dựng CPG Streaming Pipeline)

Đồ án này xây dựng một hệ thống xử lý dữ liệu luồng (real-time streaming) để trích xuất Đồ thị Thuộc tính Mã (Code Property Graph - CPG) từ mã nguồn Python (`huggingface/diffusers`), sau đó đẩy dữ liệu đồ thị vào **Neo4j** và siêu dữ liệu (metadata) vào **MongoDB** thông qua **Apache Kafka** và **Apache Spark**.

---

## 🛠️ 1. Yêu cầu Hệ thống (Prerequisites)
- **Docker & Docker Compose** (để chạy Kafka, Neo4j, MongoDB).
- **Python 3.8+** (để chạy Parser và Spark).
- **Java 8 hoặc 11** (Yêu cầu bắt buộc để chạy PySpark trên môi trường Local/WSL).

Cài đặt các thư viện Python cần thiết:
```bash
pip install -r src/parser/requirements.txt
pip install pyspark==3.5.0
```

*(Lưu ý: Bạn cần clone repo `huggingface/diffusers` vào thư mục gốc trước khi chạy để có dữ liệu phân tích).*

---

## 🚀 2. Hướng dẫn Khởi động Hệ thống

### Bước 1: Khởi động các Cơ sở dữ liệu và Kafka (Infrastructure)
Hệ thống sử dụng Docker Compose để tự động triển khai môi trường. Mở terminal tại thư mục gốc và chạy:
```bash
docker-compose up -d
```
Quá trình này sẽ khởi động:
- Zookeeper & Kafka Broker (Port: `9092`)
- Kafka Connect (Port: `8083`)
- Neo4j (Port: `7474`, `7687`)
- MongoDB (Port: `27017`)
- Kafka UI (Port: `8888`)

### Bước 2: Kích hoạt Neo4j Kafka Connector (Sink)
Đợi khoảng 1-2 phút cho Kafka Connect khởi động xong, chạy 2 lệnh sau để tạo luồng dữ liệu tự động từ Kafka thẳng vào Neo4j (bỏ qua Spark):

**Cấu hình luồng Node:**
```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @src/kafka_connect/neo4j-sink-node.json
```

**Cấu hình luồng Edge:**
```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @src/kafka_connect/neo4j-sink-edge.json
```

---

## 🏃 3. Hướng dẫn Chạy Pipeline Luồng dữ liệu

### Bước 3: Khởi chạy Spark Structured Streaming (Tiêu thụ Metadata)
Mở một Terminal mới (khuyên dùng Ubuntu/WSL để tránh lỗi cấu hình winutils của Hadoop trên Windows), và chạy Job Spark:
```bash
python3 src/streaming/mongo_ingestion.py
```
Spark sẽ liên tục lắng nghe topic `source_metadata_events` và tự động cập nhật vào MongoDB. Trạng thái (offset) được lưu tự động xuống ổ cứng tại thư mục `spark_checkpoints/`.

### Bước 4: Khởi chạy Parser Service (Sản xuất dữ liệu)
Mở một Terminal khác, chạy Parser để bắt đầu đọc từng file `.py` và đẩy lên Kafka:
```bash
python3 src/parser/parser_service.py --repo-dir diffusers/src/diffusers --kafka-broker localhost:9092
```

Lúc này, bạn có thể quan sát luồng dữ liệu chảy vào:
1. **Neo4j** (`http://localhost:7474` - user: `neo4j` / pass: `password`)
2. **MongoDB** (Dùng MongoDB Compass kết nối `mongodb://admin:password@localhost:27017`)
3. **Kafka UI** (`http://localhost:8888`)

---

## 🧪 4. Kiểm chứng tính Lũy đẳng (Idempotent)
Hệ thống được thiết kế để không tạo ra bản ghi trùng lặp nếu chạy lại.
Bạn có thể sửa đổi ngẫu nhiên 1 file trong thư mục `diffusers/` và chạy lại lệnh ở **Bước 4**. Dữ liệu sẽ tự động được cập nhật (MERGE/UPSERT) thay vì tạo ra dòng dữ liệu mới.
