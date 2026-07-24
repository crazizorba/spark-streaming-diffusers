# Task 6: Idempotent Replay Verification

*(Cả nhóm cùng điền vào sau khi Test)*
- Thay đổi 1 file trong diffusers, chạy lại Parser Service.
- Quan sát DB không có dữ liệu trùng lặp.

## Reflection
Hệ thống chứng minh được tính idempotent nhờ các stable ID (node_id, edge_id).
