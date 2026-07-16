import os
import glob

def find_python_files(repo_path):
    """
    Duyệt thư mục và tìm các file .py, loại trừ các thư mục tests, docs, setup.py
    """
    excluded_dirs = ['tests', 'docs', 'utils']
    excluded_files = ['setup.py']
    
    python_files = []
    
    for root, dirs, files in os.walk(repo_path):
        # Bỏ qua các thư mục bị loại trừ
        dirs[:] = [d for d in dirs if d not in excluded_dirs and not d.startswith('.')]
        
        for file in files:
            if file.endswith('.py') and file not in excluded_files:
                full_path = os.path.join(root, file)
                python_files.append(full_path)
                
    return python_files

if __name__ == "__main__":
    repo_dir = "diffusers"
    
    if not os.path.exists(repo_dir):
        print(f"Thư mục '{repo_dir}' không tồn tại. Vui lòng clone repository trước.")
        exit(1)
        
    print(f"Đang duyệt thư mục '{repo_dir}' để tìm các file Python...")
    
    py_files = find_python_files(repo_dir)
    
    print(f"\nTổng số file Python tìm thấy: {len(py_files)}\n")
    
    # In ra 10 file đầu tiên làm mẫu
    print("Danh sách 10 file đầu tiên:")
    for f in py_files[:10]:
        print(f" - {f}")
    
    if len(py_files) > 10:
        print(" ... (và nhiều file khác)")
    
    # Lưu danh sách ra file text
    output_file = "python_files_list.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        for py_file in py_files:
            f.write(f"{py_file}\n")
            
    print(f"\nĐã lưu danh sách toàn bộ file vào '{output_file}'")
