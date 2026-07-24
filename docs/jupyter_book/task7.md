# Task 7: Sơ đồ Kiến trúc Hệ thống (Architecture Diagram)

Dưới đây là sơ đồ tổng quan mô tả kiến trúc và luồng dữ liệu (Data Pipeline) của hệ thống trích xuất Code Property Graph (CPG) theo thời gian thực.

> **[📸 YÊU CẦU CHỤP ẢNH SƠ ĐỒ TẠI ĐÂY]** Bạn hãy chèn bức ảnh sơ đồ kiến trúc vào thay thế cho dòng chữ này nhé (lưu ảnh vào thư mục `images/` và dùng cú pháp `![Sơ đồ Kiến trúc](images/ten_anh.png)`).

**Giải thích luồng dữ liệu:**
Mã nguồn Python sau khi được clone về sẽ được **Parser Service** quét và trích xuất thành các phần tử đồ thị (Node, Edge) và siêu dữ liệu (Metadata). Các phần tử này lập tức được đẩy lên **Apache Kafka** phân vào 4 topics độc lập. Từ đây, dữ liệu rẽ làm hai hướng: **Neo4j Kafka Connector** tự động hút Node/Edge đưa thẳng vào đồ thị Neo4j bằng lệnh `MERGE` chống trùng lặp; trong khi đó **Apache Spark Streaming** sẽ liên tục tiêu thụ các gói Metadata và cập nhật (Upsert) chúng vào **MongoDB**.
