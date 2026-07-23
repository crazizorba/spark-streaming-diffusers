# Task 2: Incremental CPG Parser Service

**Tác giả:** Sang (Data Engineer - Python Core)  
**Thư mục làm việc:** `sang/`

---

## 📂 Cấu trúc thư mục `sang/`

```text
sang/
├── plan.md              # Kế hoạch chi tiết công việc của Sang & Checklist
├── requirements.txt     # Phụ thuộc thư viện Python (kafka-python-ng, jsonschema)
├── cpg_parser.py        # Core trích xuất CPG (AST, CFG, DFG, CALLS) & Stable Hash IDs
├── parser_service.py    # Service đọc incremental file Python & phát Kafka events
├── test_idempotency.py  # Script kiểm thử tính Idempotency (ID trùng khớp 100%)
└── README.md            # Hướng dẫn sử dụng
```

---

## 🛠️ 1. Chuẩn bị Môi trường

```bash
pip install -r sang/requirements.txt
```

---

## 🚀 2. Hướng dẫn Chạy Parser Service

### A. Chạy thử nghiệm không cần Kafka Server (Dry-run mode)
```bash
# Parse 10 file đầu tiên để test
python3 sang/parser_service.py --repo-dir diffusers/src/diffusers --dry-run --limit 10

# Parse toàn bộ repository diffusers
python3 sang/parser_service.py --repo-dir diffusers/src/diffusers --dry-run
```

### B. Chạy thật đẩy dữ liệu vào Kafka Broker
```bash
python3 sang/parser_service.py --repo-dir diffusers/src/diffusers --kafka-broker localhost:9092
```

---

## 🧪 3. Chạy Kiểm thử Tính Idempotent

```bash
python3 sang/test_idempotency.py
```
Khối kiểm thử sẽ tự động chạy 2 lần trên cùng một file source code và kiểm tra sự trùng khớp 100% của toàn bộ `node_id`, `edge_id` và `file_hash`.

---

## 📊 4. Các Kafka Topics & JSON Schema tương thích

Service sẽ tự động phát events tới 4 topics chuẩn hoá theo thiết kế của Tý (`Ty/schemas/`):

1. **`node_events`**: Node ID, Label, Type, File Path, Line, Offset.
2. **`edge_events`**: Edge ID, Source Node ID, Target Node ID, Edge Type (`AST`, `CFG`, `DFG`, `CALLS`).
3. **`source_metadata_events`**: File Path, File Hash (MD5), Lines of Code, Status, Last Modified.
4. **`parser_error_events`**: File Path, Error Type, Error Message, Stack Trace.
