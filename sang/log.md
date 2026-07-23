# Nhật ký Kiểm thử & Thực thi Real-Time Streaming Pipeline (Sang - Task 2)

**Hệ điều hành:** Windows (PowerShell)  
**Môi trường:** Python 3.8 (`conda activate py38`)  
**Thư viện Core:** `kafka-python-ng`, `jsonschema`  
**Hạ tầng:** Docker Desktop (WSL 2 Backend) - Apache Kafka 7.5, ZooKeeper, Kafka Connect, Neo4j 5.20, MongoDB 6.0  
**Ngày hoàn thành:** 2026-07-23  

---

## 🏗️ 1. Kiến trúc Hệ thống Real-Time Streaming Pipeline

Hệ thống được thiết kế theo đúng chuẩn kiến trúc **Event-Driven Real-Time Data Pipeline**:

```
[ Python Source Code (.py) ]
             │
             ▼ (AST Parsing & Deterministic Hash ID)
┌──────────────────────────────────────────┐
│  Incremental CPG Parser Service (Task 2) │
└────────────────────┬─────────────────────┘
                     │ (JSON Events)
                     ▼
           ┌──────────────────┐
           │   Apache Kafka   │
           │  (Broker 9092)   │
           └─────────┬────────┘
                     │
    ┌────────────────┼────────────────┐
    │                │                │
    ▼                ▼                ▼
[ Topic: ]       [ Topic: ]       [ Topic: ]
node_events      edge_events      source_metadata_events
    │                │
    └────────┬───────┘
             ▼
┌──────────────────────────────────────────┐
│     Kafka Connect (Neo4j Sink Plugin)    │
└────────────────────┬─────────────────────┘
                     │ (Cypher Sink Statements)
                     ▼
             ┌──────────────┐
             │ Neo4j Graph  │
             │ (Port 7474)  │
             └──────────────┘
```

---

## ⚡ 2. Thực nghiệm Kiểm thử Incremental Real-Time Streaming (Khi thay đổi 1 File)

### 📊 Kịch bản Thực nghiệm Trực tiếp:
- **Thao tác:** Chỉnh sửa 1 file nguồn `diffusers/src/diffusers/__init__.py` (thêm hàm `def stream_test_incremental_function(): pass`).
- **Lệnh thực thi:**
  ```powershell
  python sang/parser_service.py --file-list Thi/python_files_list.txt --kafka-broker localhost:9092
  ```

### ⏱️ Kết quả đo lường Thực tế:
- **Tổng số file trong danh sách:** 1,338 files
- **Số file bị bỏ qua (Unchanged - Skip):** **1,336 files** (quét kiểm tra hash trong mili-giây)
- **Số file được parse & phát luồng:** **Chỉ 1 file duy nhất** (`__init__.py`)
- **Số lượng Event phát lên Kafka:** **3,158 Nodes** và **3,505 Edges**
- **⏰ TỔNG THỜI GIAN THỰC THI TOÀN BỘ:** **`10.23 giây`** (thay vì 3.5 tiếng như khi quét mới từ đầu).

---

## 🧪 3. Kết quả Thực thi Chi tiết Toàn bộ Dự án (Final Execution Metrics)

- **Tổng số file Python trong danh sách:** **1,338 files** (từ `Thi/python_files_list.txt`)
- **Số file xử lý thành công:** **1,337 files**
- **Tổng số Nodes được sinh ra & phát đi:** **3,855,791 Nodes**
- **Tổng số Edges được sinh ra & phát đi:** **4,553,942 Edges**
- **Idempotency & Deterministic Hashing:** 100% các Node ID và Edge ID được tạo ra ổn định duy nhất bằng SHA-256 / MD5 hashing.
- **Tình trạng nạp vào Neo4j:** Kafka Connect đang liên tục tiêu thụ message stream và lưu vết tự động vào Neo4j Database.

---

## 🌐 4. Địa chỉ Giám sát & Truy vấn (Monitoring Endpoints)

1. 🌐 **Kafka UI (Message Inspector):** [`http://localhost:8888`](http://localhost:8888)
2. 🌐 **Neo4j Browser (Graph Visualizer):** [`http://localhost:7474`](http://localhost:7474) (User: `neo4j` | Pass: `password`)
3. 🔌 **Kafka Connect REST API:** [`http://localhost:8083/connectors`](http://localhost:8083/connectors)
