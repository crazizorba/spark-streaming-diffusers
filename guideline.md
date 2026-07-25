# 📘 Hướng Dẫn Chạy Thủ Công — Incremental CPG Pipeline

> **Cập nhật lần cuối:** 2026-07-25 | **Nhánh hiện tại:** `develop`  
> **Commit mới nhất:** `ad800ea` — Tối ưu hóa APOC, cải thiện CFG/DFG

---

## 🏗️ Kiến Trúc Tổng Quan

```
[Mã nguồn Python (diffusers)]
         ↓
  [Parser Service]  ← đọc file .py, tạo ra CPG (AST + CFG + DFG + Call graph)
         ↓
    [Apache Kafka]  ← message broker (4 topics)
       ↙        ↘
[Kafka Connect]  [Spark Structured Streaming]
  (Neo4j Sink)       (MongoDB Sink)
       ↓                   ↓
   [Neo4j]             [MongoDB]
  (Đồ thị CPG)    (Metadata mỗi file .py)
```

### Luồng dữ liệu qua các Kafka Topic

| Topic Kafka | Dữ liệu | Đến đâu |
|---|---|---|
| `node_events` | Nodes của đồ thị (hàm, class, biến...) | Neo4j (qua Kafka Connect) |
| `edge_events` | Cạnh kết nối (AST, CFG, DFG, CALLS) | Neo4j (qua Kafka Connect + APOC) |
| `source_metadata_events` | Metadata file (hash, LOC, trạng thái parse) | MongoDB (qua Spark) |
| `parser_error_events` | Lỗi parse | Log |

---

## ⚠️ Yêu Cầu Bắt Buộc

> **KHÔNG chạy trên Windows PowerShell trực tiếp!**  
> Dự án yêu cầu chạy trong **Ubuntu/WSL2** vì Spark cần môi trường Linux (thiếu `winutils.exe` trên Windows).

Kiểm tra các công cụ cần thiết trong WSL2:

```bash
docker --version    # Cần có Docker
java -version       # Cần Java 8 hoặc 11
python3 --version   # Cần Python 3.8+
curl --version      # Cần curl để đăng ký Kafka Connector
```

---

## 🚀 Thực Hiện Từng Bước

### Bước 0 — Mở WSL2, vào thư mục dự án

```bash
cd /mnt/c/TONGHOPTRENLOP/HK6/NM\ DLL/Lab4
```

> 💡 **Giải thích:** WSL2 mount ổ C: vào `/mnt/c/`. Dấu `\` trước khoảng trắng là để escape ký tự space trong shell.

---

### Bước 1 — Cài đặt thư viện Python

```bash
# Cài thư viện cho Parser (kafka-python-ng, jsonschema)
python3 -m pip install -r requirements.txt --break-system-packages

# Cài PySpark để chạy Spark Streaming
python3 -m pip install pyspark==3.5.0 --break-system-packages

# Cài thư viện kiểm chứng (Task 6 - truy vấn Neo4j/MongoDB)
python3 -m pip install neo4j pymongo --break-system-packages
```

> 💡 **Giải thích:** Cờ `--break-system-packages` chỉ cần thiết trên Ubuntu mới (Python 3.11+) vì hệ thống chặn pip theo chuẩn PEP 668. Không ảnh hưởng đến hoạt động của thư viện.

---

### Bước 2 — Clone mã nguồn mục tiêu (diffusers)

```bash
git clone https://github.com/huggingface/diffusers.git --depth=1
```

> 💡 **Giải thích:** Đây là bộ mã nguồn Python mà pipeline sẽ phân tích để tạo CPG. Tham số `--depth=1` chỉ tải commit mới nhất, tiết kiệm thời gian và dung lượng (không kéo toàn bộ lịch sử git).

---

### Bước 3 — Khởi động hạ tầng bằng Docker

```bash
docker-compose up -d
```

> 💡 **Giải thích:** Lệnh này khởi động **7 container** chạy ngầm (`-d` = detached mode):
>
> | Container | Image | Vai trò | Port |
> |---|---|---|---|
> | `cpg_zookeeper` | confluentinc/cp-zookeeper:7.5.0 | Quản lý cluster cho Kafka | 2181 |
> | `cpg_kafka` | confluentinc/cp-kafka:7.5.0 | Message broker chính | 9092 |
> | `cpg_kafka_ui` | provectuslabs/kafka-ui:v0.7.2 | Giao diện web xem Kafka | **8888** |
> | `cpg_init_kafka` | confluentinc/cp-kafka:7.5.0 | Tự tạo 4 topics rồi thoát | — |
> | `cpg_neo4j` | neo4j:5.20.0 + APOC | Graph database | **7474**, 7687 |
> | `cpg_kafka_connect` | confluentinc/cp-kafka-connect:7.5.0 | Cầu nối Kafka → Neo4j | **8083** |
> | `cpg_mongodb` | mongo:6.0 | NoSQL database cho metadata | 27017 |

**⏳ Đợi 2–3 phút** để `kafka-connect` hoàn tất tải Neo4j connector plugin từ Confluent Hub.

Kiểm tra tất cả đã sẵn sàng:
```bash
docker-compose ps
# Tất cả service phải ở trạng thái "Up"

# Kiểm tra Kafka Connect đã sẵn sàng:
curl http://localhost:8083/connectors
# Trả về [] là OK
```

---

### Bước 4 — Tạo Index cho Neo4j (tối ưu hiệu suất)

```bash
python3 -c "
from neo4j import GraphDatabase
driver = GraphDatabase.driver('bolt://127.0.0.1:7687', auth=('neo4j', 'password'))
with driver.session() as session:
    session.run('CREATE INDEX IF NOT EXISTS FOR (n:CodeNode) ON (n.node_id)')
print('Tạo Index thành công!')
driver.close()
"
```

> 💡 **Giải thích:** Khi pipeline đẩy hàng triệu node vào Neo4j, mỗi lệnh `MERGE` phải tìm node theo `node_id`. Không có index → phải scan toàn bộ (O(n)) → cực kỳ chậm. Index biến việc tìm kiếm thành O(log n), giảm thời gian nạp từ hàng giờ xuống vài phút.

---

### Bước 5 — Đăng ký Kafka Connect Sink cho Neo4j

```bash
# Đăng ký connector cho NODES (node_events → Neo4j)
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @infra/kafka_connect/neo4j_sink_node.json

# Đăng ký connector cho EDGES (edge_events → Neo4j, dùng APOC)
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @infra/kafka_connect/neo4j_sink_edge.json
```

> 💡 **Giải thích:**
> - **Node connector**: Dùng `MERGE (n:CodeNode {node_id: ...}) SET n += properties` — đảm bảo không tạo trùng node.
> - **Edge connector** (cập nhật mới trong `develop`): Dùng **APOC** (`apoc.merge.relationship`) thay vì Cypher thuần túy. APOC cho phép tạo relationship với **type động** (từ biến `event.edge_type`), giúp phân biệt đúng các loại cạnh AST/CFG/DFG/CALLS thay vì hardcode là `CALLS`.

Xác nhận connector đã chạy:
```bash
curl http://localhost:8083/connectors
# Phải thấy: ["neo4j-sink-node","neo4j-sink-edge"]

curl http://localhost:8083/connectors/neo4j-sink-node/status
# "state": "RUNNING" là thành công
```

---

### Bước 6 — Khởi chạy Spark Streaming *(mở Terminal WSL2 mới)*

```bash
cd /mnt/c/TONGHOPTRENLOP/HK6/NM\ DLL/Lab4
python3 src/streaming/mongo_ingestion.py
```

Hoặc nếu muốn override cấu hình qua biến môi trường (tính năng mới trong `develop`):
```bash
# Tùy chọn: override địa chỉ Kafka/MongoDB nếu khác mặc định
KAFKA_BROKERS="127.0.0.1:9092" \
MONGO_URI="mongodb://admin:password@127.0.0.1:27017/cpg_db.source_metadata?authSource=admin" \
python3 src/streaming/mongo_ingestion.py
```

> 💡 **Giải thích:** Script này khởi động **Spark Structured Streaming job**, lắng nghe topic `source_metadata_events`. Mỗi khi Parser gửi metadata của một file, Spark nhận, parse JSON và **upsert** vào MongoDB (collection `cpg_db.source_metadata`), dùng `file_path` làm `_id` để tránh tạo document trùng.
>
> **Lần đầu chạy:** Spark tự tải 2 thư viện từ Maven (~1–2 phút):
> - `spark-sql-kafka-0-10_2.12:3.5.0`
> - `mongo-spark-connector_2.12:10.4.0`
>
> Khi thấy dòng: `Started Structured Streaming query reading from source_metadata_events to MongoDB...` → Spark đã sẵn sàng, **giữ nguyên terminal này**.

---

### Bước 7 — Chạy Parser Service *(mở Terminal WSL2 mới)*

```bash
cd /mnt/c/TONGHOPTRENLOP/HK6/NM\ DLL/Lab4
python3 src/parser/parser_service.py \
  --repo-dir diffusers/src/diffusers \
  --kafka-broker 127.0.0.1:9092
```

> 💡 **Giải thích từng thành phần:**
>
> **`cpg_parser.py`** — Bộ phân tích AST (cập nhật mới trong `develop`):
> - Dùng Python `ast` module để parse từng file `.py` thành AST.
> - Trích xuất **4 loại cạnh đồ thị**:
>   - **AST**: Quan hệ cha-con trong cây cú pháp
>   - **CFG** (Control Flow Graph): Luồng thực thi tuần tự; **mới**: bổ sung nhánh rẽ `True/False` cho `if/for/while`
>   - **DFG** (Data Flow Graph): Luồng dữ liệu biến; **mới**: bổ sung tracking `ast.arg` (tham số hàm)
>   - **CALLS**: Quan hệ gọi hàm
>
> **`parser_service.py`** — Orchestrator:
> - Duyệt qua tất cả file `.py` trong `diffusers/src/diffusers`
> - Kiểm tra **cache MD5** (`data/.parse_cache.json`): file chưa thay đổi → bỏ qua
> - Đẩy events lên 4 Kafka topics

Output mẫu khi chạy:
```
==================================================
 Incremental CPG Parser Service (Sang)
 Target Source: Repo Dir: .../diffusers/src/diffusers
 Discovered .py files: 450 (Processing: 450)
==================================================
[20/450] Processed: modeling_unet_2d.py | Skipped: 0 | Nodes: 15234 | Edges: 8901
```

---

### Bước 8 — Kiểm tra kết quả

| Giao diện | URL / Lệnh | Cách kiểm tra |
|---|---|---|
| **Kafka UI** | http://localhost:8888 | Xem messages đang chảy qua topics |
| **Neo4j Browser** | http://localhost:7474 | Login `neo4j`/`password` |
| **MongoDB** | `mongosh "mongodb://admin:password@localhost:27017"` | Xem collection metadata |

**Truy vấn Neo4j mẫu:**
```cypher
// Đếm tổng số nodes theo loại
MATCH (n:CodeNode) RETURN n.type, count(*) ORDER BY count(*) DESC

// Xem các hàm trong đồ thị
MATCH (n:CodeNode {type: 'FUNCTION'}) RETURN n.name, n.file_path LIMIT 10

// Xem cạnh CFG (luồng điều khiển)
MATCH (a:CodeNode)-[r:CFG]->(b:CodeNode) RETURN a, r, b LIMIT 20
```

**Truy vấn MongoDB mẫu:**
```javascript
use cpg_db
db.source_metadata.find({parse_status: "SUCCESS"}).count()
db.source_metadata.find().sort({loc: -1}).limit(5)
```

---

## 🔄 Test Tính Lũy Đẳng (Idempotent)

```bash
# 1. Dừng Parser (Ctrl+C ở Terminal bước 7)

# 2. Sửa một file bất kỳ
echo "# test comment" >> diffusers/src/diffusers/__init__.py

# 3. Chạy lại Parser
python3 src/parser/parser_service.py \
  --repo-dir diffusers/src/diffusers \
  --kafka-broker 127.0.0.1:9092

# Kết quả kỳ vọng: CHỈ 1 file bị parse lại, 449 file "Skipped (Unchanged)"
```

---

## 🛠️ Tùy Chọn Hữu Ích Khi Chạy Parser

```bash
# Thử nghiệm không gửi lên Kafka
python3 src/parser/parser_service.py --repo-dir diffusers/src/diffusers --dry-run

# Giới hạn chỉ xử lý 10 file (test nhanh)
python3 src/parser/parser_service.py \
  --repo-dir diffusers/src/diffusers \
  --kafka-broker 127.0.0.1:9092 \
  --limit 10

# Bắt buộc parse lại tất cả (bỏ qua cache)
python3 src/parser/parser_service.py \
  --repo-dir diffusers/src/diffusers \
  --kafka-broker 127.0.0.1:9092 \
  --force-reparse
```

---

## 🛑 Dừng Hệ Thống

```bash
# Dừng Spark và Parser: nhấn Ctrl+C trong từng terminal

# Dừng Docker (giữ nguyên data)
docker-compose down

# Xóa luôn toàn bộ data (volumes)
docker-compose down -v
```

---

## 📝 Thay Đổi Từ Nhánh `develop` (commit `ad800ea`)

| File | Thay đổi |
|---|---|
| `src/parser/cpg_parser.py` | Bổ sung tracking `ast.arg` (tham số hàm) cho DFG; Thêm CFG branch `True/False` cho `if/for/while` |
| `src/streaming/mongo_ingestion.py` | MongoDB URI và Kafka broker đọc từ biến môi trường `MONGO_URI`, `KAFKA_BROKERS` |
| `infra/kafka_connect/neo4j_sink_edge.json` | Dùng `apoc.merge.relationship` — edge type động thay vì hardcode `CALLS` |
| `docs/jupyter_book/task2.ipynb` | Cập nhật nội dung báo cáo |
