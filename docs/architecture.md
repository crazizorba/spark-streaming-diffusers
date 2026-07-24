# Architecture Diagram cho Lab 04: Spark Streaming

Sơ đồ dưới đây mô tả luồng dữ liệu tổng thể của hệ thống từ lúc đọc mã nguồn đến khi lưu trữ vào cơ sở dữ liệu.

```mermaid
graph TD
    A[GitHub Repository<br/>huggingface/diffusers] -->|git clone| B(Local File System)
    
    subgraph Parser Service
        B -->|Read .py files| C[Incremental CPG Parser<br/>Python Service]
        C -->|Extract AST| C1(AST Nodes)
        C -->|Extract CFG| C2(CFG Edges)
        C -->|Extract DFG| C3(DFG Edges)
        C -->|Extract Calls| C4(Call Edges)
        C -->|Extract Metadata| C5(Source Metadata)
    end
    
    subgraph Apache Kafka Cluster
        C1 -.->|JSON/Avro + Schema Version| K1[(Topic: node_events)]
        C2 -.->|JSON/Avro + Schema Version| K2[(Topic: edge_events)]
        C3 -.->|JSON/Avro + Schema Version| K2
        C4 -.->|JSON/Avro + Schema Version| K2
        C5 -.->|JSON/Avro + Schema Version| K3[(Topic: source_metadata_events)]
        C -.->|Errors| K4[(Topic: parser_error_events)]
    end
    
    subgraph Storage & Stream Processing
        K1 ===>|Neo4j Kafka Connector Sink<br/>Idempotent MERGE| N[(Neo4j Graph Database)]
        K2 ===>|Neo4j Kafka Connector Sink<br/>Idempotent MERGE| N
        
        K3 ===>|Consume| S[Apache Spark<br/>Structured Streaming]
        S ===>|MongoDB Spark Connector<br/>Idempotent Upsert| M[(MongoDB)]
        
        S -.->|Maintain Offset| CP((Checkpoint Location))
    end
    
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style C fill:#bbf,stroke:#333,stroke-width:2px
    style K1 fill:#ff9,stroke:#333,stroke-width:2px
    style K2 fill:#ff9,stroke:#333,stroke-width:2px
    style K3 fill:#ff9,stroke:#333,stroke-width:2px
    style K4 fill:#f99,stroke:#333,stroke-width:2px
    style N fill:#9f9,stroke:#333,stroke-width:2px
    style S fill:#fbb,stroke:#333,stroke-width:2px
    style M fill:#9f9,stroke:#333,stroke-width:2px
```

## Giải thích luồng dữ liệu:
1. **Source:** Mã nguồn được tải về từ GitHub.
2. **Parser Service:** Đọc tuần tự từng file Python, phân tích và trích xuất các thành phần đồ thị (Nodes, Edges) cùng với siêu dữ liệu (Metadata). Mỗi thành phần được gán một ID cố định (Stable ID).
3. **Kafka:** Hoạt động như một Message Broker trung tâm, nhận các sự kiện từ Parser Service và phân loại vào 4 topics khác nhau.
4. **Neo4j Ingestion:** Sử dụng Neo4j Kafka Connector Sink để hút dữ liệu trực tiếp từ các topic `node_events` và `edge_events`. Lệnh MERGE được sử dụng để đảm bảo tính Idempotent (không trùng lặp khi chạy lại).
5. **MongoDB Ingestion:** Apache Spark Structured Streaming đọc dữ liệu từ topic `source_metadata_events`, xử lý và lưu vào MongoDB thông qua MongoDB Spark Connector. Trạng thái đọc (offset) được lưu tại Checkpoint Location để có thể phục hồi khi có sự cố.
