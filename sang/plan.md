# Kế hoạch & Hướng dẫn Thực hiện: Incremental CPG Parser Service (Task 2)
**Người thực hiện:** Sang (Data Engineer - Python Core)  
**Mục tiêu:** Xây dựng Python Parser Service đọc từng file Python trong repository, trích xuất Code Property Graph (AST, CFG, DFG, Call edges), tạo Stable Identifier (đảm bảo tính Idempotent) và gửi các event dạng JSON đến Apache Kafka Topics chuẩn hoá theo thiết kế của Tý (Task 3).

---

## 1. Những gì cần chuẩn bị (Preparations)

### 1.1 Môi trường Python & Thư viện
*   **Python:** >= 3.8
*   **Cài đặt các gói phụ thuộc (Dependencies):**
    ```bash
    pip install -r sang/requirements.txt
    ```
    *(Gồm `kafka-python-ng` và `jsonschema`)*.

### 1.2 Môi trường Kafka (Phối hợp với Tý - Task 3)
*   Cụm Kafka Broker running (mặc định: `localhost:9092` hoặc `kafka:9092` trong Docker).
*   Các Kafka Topics đã được khởi tạo:
    1.  `node_events` - Chứa thông tin các nút của CPG (AST, Function, Class, Variable...).
    2.  `edge_events` - Chứa các cạnh quan hệ (AST parent-child, CFG, DFG, CALLS).
    3.  `source_metadata_events` - Chứa metadata file (file_path, file_hash, loc...) đẩy cho Spark -> MongoDB (Task 5).
    4.  `parser_error_events` - Log lỗi parse nếu gặp syntax error hoặc file hỏng.

### 1.3 Thống nhất Schema Data (Khớp 100% với `Ty/schemas/`)
| Field | Type | Description |
| :--- | :--- | :--- |
| **Node Event** | | |
| `schema_version` | String | `"1.0"` |
| `event_time` | String (ISO8601) | e.g. `"2026-07-23T13:00:00Z"` |
| `node_id` | String (Hash) | Stable ID: `sha256(file_path + type + line + col + name)` |
| `node_label` | String | Label của Node (`AST_NODE`, `FUNCTION`, `CLASS`, `VARIABLE`, `EXPRESSION`...) |
| `properties` | Object | `{"file_path": ..., "line_number": ..., "col_offset": ..., "name": ...}` |
| **Edge Event** | | |
| `schema_version` | String | `"1.0"` |
| `event_time` | String (ISO8601) | ISO 8601 timestamp |
| `edge_id` | String (Hash) | Stable ID: `sha256(source_id + type + target_id)` |
| `source_node_id` | String | Node ID bắt đầu |
| `target_node_id` | String | Node ID kết thúc |
| `edge_type` | String | `AST`, `CFG`, `DFG`, `CALLS` |
| `properties` | Object | Thuộc tính mở rộng nếu có |

---

## 2. Kế hoạch triển khai công việc (Tasks & Checklist)

- [x] **Phase 1: Khởi tạo Cấu trúc & Kế hoạch (`sang/plan.md`)**
- [x] **Phase 2: Triển khai CPG Parser Core (`sang/cpg_parser.py`)**
    - [x] Xây dựng thuật toán sinh **Stable Identifiers** (Deterministic Hashes) cho Node và Edge.
    - [x] Trích xuất **AST Nodes & AST Edges** sử dụng `ast.NodeVisitor` của Python.
    - [x] Trích xuất **CFG Edges** (Control Flow Graph) tuần tự giữa các câu lệnh trong hàm/khối lệnh.
    - [x] Trích xuất **DFG Edges** (Data Flow Graph) giữa vị trí gán biến (`Store`) và vị trí đọc biến (`Load`).
    - [x] Trích xuất **Call Edges** từ các node `ast.Call` tới hàm tương ứng.
    - [x] Trích xuất **Source Metadata** (MD5 hash, LOC, last modified timestamp).
- [x] **Phase 3: Triển khai Kafka Producer Service (`sang/parser_service.py`)**
    - [x] Đọc từng file `.py` tăng dần (incremental / bounded memory).
    - [x] Validate message bằng JSON Schema trước khi bắn.
    - [x] Bắn message vào 4 Kafka Topics tương ứng (`node_events`, `edge_events`, `source_metadata_events`, `parser_error_events`).
    - [x] Hỗ trợ chế độ `--dry-run` (chạy kiểm thử không cần Kafka Broker) để kiểm tra đầu ra JSON.
- [x] **Phase 4: Kiểm thử & Đánh giá (Verification & Idempotency Check)**
    - [x] Chạy Parser trên 5 file mẫu thuộc `diffusers/src/diffusers/` (4958 nodes, 5623 edges, 0 errors).
    - [x] Kiểm tra tính Idempotent (`sang/test_idempotency.py`): Parse cùng 1 file 2 lần $\rightarrow$ Node ID và Edge ID trùng khớp 100%.
    - [ ] Phối hợp với Thi & Tý để test end-to-end (bắn Kafka $\rightarrow$ Neo4j & MongoDB).

---

## 3. Hướng dẫn Chạy & Kiểm thử Code của Sang

### Chạy kiểm thử khô (Dry run - Không cần Kafka Server):
```bash
python3 sang/parser_service.py --repo-dir diffusers/src/diffusers --dry-run
```

### Chạy kiểm thử Idempotency:
```bash
python3 sang/test_idempotency.py
```

### Chạy thật đẩy dữ liệu lên Kafka:
```bash
python3 sang/parser_service.py --repo-dir diffusers/src/diffusers --kafka-broker localhost:9092
```
