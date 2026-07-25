# Đồ án Tổng kết: Incremental Code Property Graph (CPG) Pipeline

Chào mừng thầy và các bạn đến với Báo cáo Đồ án môn học Big Data!

## 1. Giới thiệu dự án (Introduction)

Đồ án này tập trung vào việc thiết kế và xây dựng một Data Pipeline hoàn chỉnh, có khả năng xử lý luồng dữ liệu thời gian thực (Real-time Streaming) kết hợp với công nghệ Đồ thị (Graph Database). 

Mục tiêu cốt lõi của hệ thống là trích xuất **Code Property Graph (CPG)** từ các kho mã nguồn Python lớn (cụ thể là thư viện [Diffusers](https://github.com/huggingface/diffusers) của Hugging Face), sau đó liên tục đẩy các nút đồ thị (Node), cạnh (Edge) và siêu dữ liệu (Metadata) vào hệ thống lưu trữ mà vẫn phải đảm bảo nghiêm ngặt tính **Lũy đẳng (Idempotent)** và xử lý **Tăng cường (Incremental)**.

### Kiến trúc công nghệ cốt lõi:
- **Ngôn ngữ & Phân tích:** Python, `ast` (Bóc tách cú pháp cây - Abstract Syntax Tree).
- **Trục xương sống (Message Broker):** Apache Kafka & Kafka Connect.
- **Lưu trữ Đồ thị (Graph Storage):** Neo4j (Lưu trữ CPG, Node, Edge).
- **Xử lý Luồng (Stream Processing):** Apache Spark Structured Streaming.
- **Lưu trữ Siêu dữ liệu (Metadata Storage):** MongoDB (Lưu trữ lịch sử, trạng thái file, LOC).

---

## 2. Danh sách thành viên nhóm

Nhóm chúng em gồm 4 thành viên, mỗi người đều nỗ lực hết sức và phối hợp ăn ý để hoàn thành toàn bộ các Task khó nhằn của đồ án này. Dưới đây là danh sách thành viên và mức độ đóng góp:

| STT | Họ và Tên | Mã Số Sinh Viên | Mức độ đóng góp |
|:---:|:---|:---:|:---:|
| 1 | **Cao Quốc Tý** | 23120400 | 100% |
| 2 | **Trần Đình Thi** | 23120359 | 100% |
| 3 | **Cao Quốc Tuấn** | 23120390 | 100% |
| 4 | **Hoàng Văn Sang** | 23120350 | 100% |

Cảm ơn thầy cô đã hướng dẫn tận tình để nhóm có thể hoàn thành xuất sắc hệ thống Big Data phức tạp này!

> 🚀 **Hướng dẫn xem báo cáo:** Bạn có thể chuyển hướng giữa các Task ở thanh menu bên trái màn hình.
