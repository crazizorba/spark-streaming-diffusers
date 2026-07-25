# Đồ án Tổng kết: Incremental Code Property Graph (CPG) Pipeline

Đồ án này xây dựng một hệ thống xử lý dữ liệu luồng (real-time streaming) để trích xuất **Đồ thị Thuộc tính Mã (Code Property Graph - CPG)** từ mã nguồn Python. Sau đó, hệ thống sẽ tự động đẩy dữ liệu đồ thị vào **Neo4j** và siêu dữ liệu (metadata) vào **MongoDB** thông qua **Apache Kafka** và **Apache Spark**.

---

##  Yêu cầu Hệ thống & Cài đặt Môi trường

### 1. Hệ điều hành & Hạ tầng
- **Docker & Docker Compose**: Yêu cầu bắt buộc để khởi động cụm hạ tầng (Kafka, Neo4j, MongoDB).
- **Hệ điều hành**: Yêu cầu chạy trên **Ubuntu (WSL2)** đối với Windows hoặc hệ điều hành Linux thuần. Vui lòng không chạy trực tiếp bằng Windows PowerShell để tránh các lỗi tương thích về đường dẫn và lỗi thiếu `winutils.exe` của Hadoop.
- **Java**: Yêu cầu máy cài sẵn **Java 8 hoặc 11** để làm môi trường nền tảng cho Apache Spark.
- **Python**: Khuyên dùng Python 3.8 trở lên.

### 2. Cài đặt Thư viện (Dependencies)
Mở Terminal Ubuntu (WSL) và thực hiện các lệnh cài đặt dưới đây. 

*(Lưu ý: Đối với Ubuntu đời mới dùng Python 3.11 trở lên, bạn phải thêm cờ `--break-system-packages` để vượt qua cơ chế bảo vệ PEP 668 của hệ điều hành).*

**Cài đặt thư viện cho Parser và Spark Streaming:**
```bash
python3 -m pip install -r src/parser/requirements.txt --break-system-packages
python3 -m pip install pyspark==3.5.0 --break-system-packages
```

**Cài đặt thư viện hỗ trợ truy vấn kiểm chứng (Task 6):**
```bash
python3 -m pip install neo4j pymongo --break-system-packages
```

---

##  HƯỚNG DẪN TRIỂN KHAI VÀ THỰC THI

Hệ thống được thiết kế để chạy tự động theo đúng luồng Data Pipeline. Hãy làm theo trình tự dưới đây.

### Bước 1: Chuẩn bị Source Code (`diffusers`)
Mở Terminal tại thư mục gốc của đồ án và chạy lệnh sau để tải kho mã nguồn mục tiêu. 
*(Lưu ý: Dùng tham số `--depth 1` để tải nhanh và tiết kiệm dung lượng)*:
```bash
git clone https://github.com/huggingface/diffusers.git --depth=1
```

### Bước 2: Khởi động Hạ tầng (Infrastructure)
Khởi động hệ thống Database và Message Broker bằng Docker Compose:
```bash
docker-compose up -d
```
Quá trình này sẽ khởi động các dịch vụ: Zookeeper, Kafka, Kafka Connect, Neo4j, và MongoDB.
> 📌 **Lưu ý:** Vui lòng đợi khoảng 1-2 phút để Kafka Connect khởi động hoàn tất trước khi thực hiện bước tiếp theo.

### Bước 3: Đánh Chỉ mục (Index) cho Neo4j
Để tối ưu hóa hiệu suất nạp dữ liệu (tránh nghẽn thắt cổ chai khi Insert hàng triệu Node), hãy đánh chỉ mục cho trường `node_id`. Chạy đoạn script Python ngắn sau:
```bash
python -c "
from neo4j import GraphDatabase
driver = GraphDatabase.driver('bolt://127.0.0.1:7687', auth=('neo4j', 'password'))
with driver.session() as session:
    session.run('CREATE INDEX IF NOT EXISTS FOR (n:CodeNode) ON (n.node_id)')
print('Tạo Index thành công!')
driver.close()
"
```

### Bước 4: Kích hoạt Sink Connector cho Neo4j
Chạy 2 lệnh `curl` sau để nạp cấu hình, tự động đẩy dữ liệu từ Kafka vào đồ thị Neo4j:
```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @infra/kafka_connect/neo4j_sink_node.json
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @infra/kafka_connect/neo4j_sink_edge.json
```
>  **Tính năng:** Connector được thiết kế sử dụng lệnh `MERGE` thay vì `CREATE` để đảm bảo tính Lũy Đẳng (chống trùng lặp dữ liệu) trong hệ thống Đồ thị.

### Bước 5: Khởi chạy Spark Structured Streaming (Tiêu thụ Metadata)
Mở một Terminal mới và kích hoạt Job Spark:
```bash
python3 src/streaming/mongo_ingestion.py
```
> 📌 **Lưu ý:** Ở lần chạy đầu tiên, Spark sẽ mất khoảng 1-2 phút để tự động tải thư viện `mongo-spark-connector` qua mạng (Maven).

### Bước 6: Bật Parser Service để Trích xuất Dữ liệu (Ingestion)
Khi Spark đã ở trạng thái chờ, hãy mở một Terminal khác và kích hoạt Parser:
```bash
python3 src/parser/parser_service.py --repo-dir diffusers/src/diffusers --kafka-broker 127.0.0.1:9092
```
Hệ thống sẽ quét từng file `.py` trong kho mã nguồn và đẩy hàng triệu event lên Kafka. Bạn sẽ ngay lập tức thấy Spark và Neo4j bắt đầu tiêu thụ dữ liệu theo thời gian thực!

---

##  Kiểm chứng Tính Lũy Đẳng (Idempotent Test)

Để kiểm chứng tính bền vững của Pipeline khi có sự thay đổi mã nguồn (Incremental Update):
1. **Dừng Parser Service** (Nhấn `Ctrl+C`).
2. Mở một file Python bất kỳ trong `diffusers` (ví dụ `diffusers/src/diffusers/__init__.py`) và thêm một đoạn code nhỏ hoặc một đoạn ghi chú (comment) mới.
3. **Chạy lại lệnh Parser Service** ở Bước 6.
4. Hệ thống sẽ phát hiện chính xác file bị sửa đổi và chỉ bóc tách lại file đó.
5. Truy cập Neo4j và MongoDB, bạn sẽ thấy hệ thống chỉ **thêm mới/ghi đè (Upsert)** các thông tin tương ứng với đoạn code vừa thêm, hoàn toàn **không nhân bản (duplicate)** các dữ liệu đã có.
