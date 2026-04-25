import os
import hashlib
import time
import subprocess
import sys
import shutil

def get_file_hash(filepath):
    """Calculate MD5 hash of a file."""
    if not os.path.isfile(filepath):
        return None
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def generate_test_structure(base_path):
    """Generate a nested folder structure for testing."""
    os.makedirs(base_path, exist_ok=True)
    
    # Create some files in root
    for i in range(3):
        with open(os.path.join(base_path, f"file_{i}.bin"), 'wb') as f:
            f.write(os.urandom(5 * 1024 * 1024)) # 5MB each
            
    # Create subfolder
    sub = os.path.join(base_path, "subfolder")
    os.makedirs(sub, exist_ok=True)
    with open(os.path.join(sub, "nested.bin"), 'wb') as f:
        f.write(os.urandom(10 * 1024 * 1024)) # 10MB
    
    print(f"Test structure generated at {base_path}")

def verify_structures(src, dst):
    """Verify that two folder structures are identical."""
    for root, dirs, files in os.walk(src):
        rel_path = os.path.relpath(root, src)
        # Handle the '.' case cleanly
        if rel_path == ".":
            target_root = dst
        else:
            target_root = os.path.join(dst, rel_path)
        
        if not os.path.exists(target_root):
            return False, f"Missing directory: {target_root}"
            
        for f in files:
            s_file = os.path.join(root, f)
            d_file = os.path.join(target_root, f)
            
            if not os.path.exists(d_file):
                return False, f"Missing file: {d_file}"
                
            s_hash = get_file_hash(s_file)
            d_hash = get_file_hash(d_file)
            if s_hash != d_hash:
                return False, f"Hash mismatch for {f}: {s_hash} != {d_hash}"
                
    return True, "All files match!"

def main():
    test_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_run")
    src_folder = os.path.join(test_dir, "source_folder")
    dst_folder = os.path.join(test_dir, "destination_folder")
    
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)
    
    # 1. Generate test structure
    generate_test_structure(src_folder)
    
    # 2. Run fast_copy.py
    fast_copy_script = os.path.join(os.path.dirname(test_dir), "fast_copy.py")
    print("\nStarting Fast Copy (Folder)...")
    
    # Pre-create dst_folder so fast_copy.py nests the source inside it
    os.makedirs(dst_folder)
    
    start_time = time.time()
    result = subprocess.run([sys.executable, fast_copy_script, src_folder, dst_folder])
    end_time = time.time()
    
    if result.returncode == 0:
        # Since dst_folder existed, actual copy is at dst_folder/source_folder
        actual_dst = os.path.join(dst_folder, "source_folder")
        
        print(f"\nCopy process took {end_time - start_time:.2f} seconds.")
        
        # 3. Verify integrity
        success, message = verify_structures(src_folder, actual_dst)
        print(f"Verification: {message}")
        
        if success:
            print("\nVERIFICATION SUCCESS: Folder structure and integrity maintained.")
        else:
            print("\nVERIFICATION FAILED!")
    else:
        print("\nCopy process failed.")

if __name__ == "__main__":
    main()
