# Nhật ký công việc - Thi (Trưởng nhóm)

Dưới đây là danh sách chi tiết các công việc và các file tôi đã xử lý. Yêu cầu các thành viên khác khi hoàn thành công việc cũng tạo một thư mục mang tên mình và file `log.md` tương tự để tiện theo dõi, review chéo và chấm điểm. Tất cả các file mã nguồn cá nhân cũng phải được lưu vào thư mục này.

## Ngày 16/07/2026

### 📝 Các file đã THÊM MỚI / CHỈNH SỬA:
1. **`plan.md`** (Lưu ở thư mục gốc repo): 
   - *Mục đích*: Kế hoạch thực hiện tổng thể cho Lab 04, liệt kê các tasks và phân công công việc chi tiết cho 4 thành viên.
   - *Tình trạng*: Hoàn thành.

2. **`Thi/architecture.md`**: 
   - *Mục đích*: Sơ đồ kiến trúc luồng dữ liệu của hệ thống, thiết kế bằng ngôn ngữ Mermaid (bao gồm GitHub -> Parser -> Kafka -> Neo4j & MongoDB).
   - *Tình trạng*: Hoàn thành.

3. **`Thi/file_discovery.py`**: 
   - *Mục đích*: Code script Python thực hiện Task 1. Duyệt repo `diffusers`, đếm số lượng file `.py` (loại bỏ thư mục tests, docs...).
   - *Tình trạng*: Chạy thành công.

4. **`Thi/python_files_list.txt`**: 
   - *Mục đích*: Output từ `file_discovery.py`, chứa danh sách 1338 đường dẫn file Python cần parse.
   - *Tình trạng*: Hoàn thành.

5. **`.gitignore`** (Lưu ở thư mục gốc repo):
   - *Mục đích*: Cấu hình loại bỏ thư mục mã nguồn gốc `diffusers/` (rất nặng) và các file tạm không cần thiết lên GitHub.
   - *Tình trạng*: Hoàn thành.

### 🛠️ Các lệnh đã chạy (Terminal Commands):
- `git clone --depth 1 https://github.com/huggingface/diffusers.git`: Tải source code mục tiêu về máy local.
- `python Thi/file_discovery.py`: Khởi chạy script đếm file.
- `git add`, `git commit`: Khởi tạo và lưu phiên bản trên nhánh `main`.

### 📌 Kế hoạch tiếp theo (Next Steps):
- Đợi các thành viên (Sang, Tý, Tuấn) hoàn thành Task 2, 3, 4, 5. Các bạn hãy clone nhánh `main` về, tự tạo thư mục mang tên mình và code vào đó nhé.
- Thực hiện **Task 6 (Idempotent Replay Verification)**: sẽ trực tiếp sửa 1 file trong source `diffusers` để test toàn bộ luồng dữ liệu từ Kafka đến Database.
