# Đồ án: Lab 04 - Spark Streaming (Xây dựng CPG Streaming Pipeline)

Đồ án này xây dựng một hệ thống xử lý dữ liệu luồng (real-time streaming) để trích xuất Đồ thị Thuộc tính Mã (Code Property Graph - CPG) từ mã nguồn Python, sau đó đẩy dữ liệu đồ thị vào **Neo4j** và siêu dữ liệu (metadata) vào **MongoDB** thông qua **Apache Kafka** và **Apache Spark**.

---

## 🛠️ Yêu cầu Hệ thống (Prerequisites)
- **Docker & Docker Compose** (Bật sẵn trên máy để chạy Kafka, Neo4j, MongoDB).
- **Môi trường:** Khuyên dùng **Ubuntu (WSL2)** để chạy mã nguồn thay vì Windows thuần nhằm tránh các lỗi phiền toái liên quan đến Hadoop `winutils.exe` khi chạy Spark.
- **Python 3.8+** và **Java 8 hoặc 11** (Để chạy PySpark).

```bash
pip install -r src/parser/requirements.txt
pip install pyspark==3.5.0
```

---

## 🚀 HƯỚNG DẪN THỰC THI TỪ TASK 1 ĐẾN TASK 5

Hệ thống được thiết kế để chạy theo đúng luồng từ Task 1 đến Task 5. Hãy làm theo trình tự dưới đây.

### 📍 Task 1: Chuẩn bị Source Code (`diffusers`)
Mở Terminal tại thư mục gốc của đồ án và chạy lệnh sau để clone kho mã nguồn mục tiêu. 
*(Lưu ý: Dùng tham số `--depth 1` để shallow-clone giúp tải siêu nhanh và tiết kiệm dung lượng)*:
```bash
git clone https://github.com/huggingface/diffusers.git --depth=1
```

### 📍 Task 2: Khởi động Hạ tầng Docker (Infrastructure)
Khởi động hệ thống DB và Kafka bằng Docker Compose. 
```bash
docker-compose up -d
```
Quá trình này sẽ khởi động Zookeeper, Kafka (9092), Kafka Connect (8083), Neo4j (7474), MongoDB (27017).
> ⚠️ **Lỗi thường gặp:** Đôi khi Kafka Connect khởi động chậm hơn Kafka. Hãy đợi khoảng 2-3 phút cho đến khi lệnh `curl localhost:8083` trả về phản hồi trước khi đi tiếp.

### 📍 Task 3: Kích hoạt Sink Connector cho Neo4j
Chạy 2 lệnh `curl` sau để nạp cấu hình tự động đẩy dữ liệu từ Kafka vào Neo4j:
```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @src/kafka_connect/neo4j-sink-node.json

curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @src/kafka_connect/neo4j-sink-edge.json
```
> ✅ **Lưu ý:** Connector đã được code sử dụng lệnh `MERGE` thay vì `CREATE` để chặn hoàn toàn việc tạo dữ liệu trùng lặp trong Đồ thị.

### 📍 Task 4: Khởi chạy Spark Structured Streaming (Tiêu thụ Metadata)
Mở một Terminal mới (trên Ubuntu/WSL) và chạy Job Spark:
```bash
python3 src/streaming/mongo_ingestion.py
```
> ⚠️ **Các lỗi đã được khắc phục ngầm trong code:**
> 1. **Lỗi IPv6 Loopback:** Spark thỉnh thoảng nhầm lẫn IP trên Windows, code đã được chốt cứng ép dùng IPv4 `127.0.0.1` để kết nối Kafka.
> 2. **Lỗi `hdfs://localhost:9000`:** Do thiếu cụm Hadoop thực tế, code đã được ép dùng `file://` để ghi Checkpoint trực tiếp xuống ổ cứng thay vì tìm HDFS.
> 3. Lần đầu chạy sẽ hơi lâu do Spark phải tải thư viện `mongo-spark-connector` qua Maven.

### 📍 Task 5: Bật Parser Service để bắn dữ liệu (Ingestion)
Khi Spark đã ở trạng thái chờ, hãy mở một Terminal khác và kích hoạt Parser:
```bash
python3 src/parser/parser_service.py --repo-dir diffusers/src/diffusers --kafka-broker 127.0.0.1:9092
```
Service sẽ quét từng file `.py` trong `diffusers/` và bắn hàng chục nghìn event lên Kafka. Bạn sẽ ngay lập tức thấy Spark ở Terminal bên kia bắt đầu chớp nháy nhận dữ liệu!

---

## 🧪 Hướng dẫn Task 6 (Kiểm thử Lũy đẳng / Idempotent)
1. Dừng Parser Service (Ctrl+C).
2. Vào thư mục `diffusers/src/diffusers`, mở một file python bất kỳ (ví dụ `__init__.py`) và sửa đổi/thêm 1 dòng comment vào đó.
3. Chạy lại lệnh Parser Service ở **Task 5**.
4. Truy cập Neo4j và MongoDB, bạn sẽ thấy tổng số lượng Node/Edge/Document không hề bị nhân đôi nhờ cơ chế `MERGE` ở Sink và `Idempotent Hash ID` của Parser!
