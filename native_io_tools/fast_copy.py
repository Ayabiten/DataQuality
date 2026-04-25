import ctypes
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from ctypes import wintypes

# Win32 Constants
COPY_FILE_ALLOW_DECRYPTED_DESTINATION = 0x00000008
COPY_FILE_RESTARTABLE = 0x00000002
PROGRESS_CONTINUE = 0
PROGRESS_CANCEL = 1

# Progress Callback Type Definition
PROGRESS_ROUTINE = ctypes.WINFUNCTYPE(
    wintypes.DWORD,
    ctypes.c_longlong, ctypes.c_longlong,
    ctypes.c_longlong, ctypes.c_longlong,
    wintypes.DWORD, wintypes.DWORD,
    wintypes.HANDLE, wintypes.HANDLE,
    ctypes.c_void_p
)

class FastCopy:
    def __init__(self, max_workers=4):
        self.kernel32 = ctypes.windll.kernel32
        self.start_time = None
        self.last_update = 0
        self.total_size = 0
        self.total_transferred = 0
        self.lock = threading.Lock()
        self.max_workers = max_workers
        # Track individual file progress for the global total
        self.file_progress = {} 

    def _progress_callback(self, total_size, transferred, stream_size, stream_transferred, 
                           dw_stream_num, reason, h_src, h_dst, data):
        # 'data' is the file identifier (index or path)
        file_id = data
        
        with self.lock:
            # Update the progress for this specific file
            prev_file_transferred = self.file_progress.get(file_id, 0)
            self.total_transferred += (transferred - prev_file_transferred)
            self.file_progress[file_id] = transferred

            now = time.time()
            if now - self.last_update > 0.1 or self.total_transferred == self.total_size:
                self.last_update = now
                elapsed = now - self.start_time
                percent = (self.total_transferred / self.total_size) * 100 if self.total_size > 0 else 100
                
                speed_mb = (self.total_transferred / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                
                bar_length = 40
                filled_length = int(bar_length * self.total_transferred // self.total_size) if self.total_size > 0 else bar_length
                bar = '#' * filled_length + '-' * (bar_length - filled_length)
                
                if speed_mb > 0:
                    remaining_bytes = self.total_size - self.total_transferred
                    remaining_seconds = (remaining_bytes / (1024 * 1024)) / speed_mb
                    eta = time.strftime("%H:%M:%S", time.gmtime(remaining_seconds))
                else:
                    eta = "--:--:--"

                sys.stdout.write(f"\rProgress: |{bar}| {percent:6.2f}% [{speed_mb:8.2f} MB/s] ETA: {eta}")
                sys.stdout.flush()

        return PROGRESS_CONTINUE

    def _get_total_size(self, path):
        if os.path.isfile(path):
            return os.path.getsize(path)
        
        total = 0
        for root, dirs, files in os.walk(path):
            for f in files:
                fp = os.path.join(root, f)
                try:
                    total += os.path.getsize(fp)
                except OSError:
                    pass
        return total

    def _copy_file_internal(self, src_path, dst_path, file_id):
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        
        callback = PROGRESS_ROUTINE(self._progress_callback)
        cancel = ctypes.c_bool(False)
        
        result = self.kernel32.CopyFileExW(
            ctypes.c_wchar_p(src_path),
            ctypes.c_wchar_p(dst_path),
            callback,
            ctypes.c_void_p(file_id),
            ctypes.byref(cancel),
            wintypes.DWORD(0)
        )
        return result

    def copy(self, src, dst):
        if not os.path.exists(src):
            print(f"Error: Source '{src}' not found.")
            return False

        src_path = os.path.abspath(src)
        dst_path = os.path.abspath(dst)
        
        # Determine if we are copying a file or a folder
        is_dir = os.path.isdir(src_path)
        
        if is_dir:
            # If copying a folder to another folder, create a subfolder
            if os.path.exists(dst_path) and os.path.isdir(dst_path):
                dst_path = os.path.join(dst_path, os.path.basename(src_path))
        else:
            # If copying a file to a directory, append filename
            if os.path.isdir(dst_path):
                dst_path = os.path.join(dst_path, os.path.basename(src_path))

        print(f"Source: {src_path}")
        print(f"Target: {dst_path}")
        
        print("Calculating total size...")
        self.total_size = self._get_total_size(src_path)
        print(f"Total Size: {self.total_size / (1024**3):.2f} GB")
        print("-" * 60)

        self.start_time = time.time()
        self.file_progress = {}
        self.total_transferred = 0

        if not is_dir:
            success = self._copy_file_internal(src_path, dst_path, 0)
        else:
            tasks = []
            file_counter = 0
            for root, dirs, files in os.walk(src_path):
                # Replicate directory structure
                rel_path = os.path.relpath(root, src_path)
                target_dir = os.path.join(dst_path, rel_path)
                os.makedirs(target_dir, exist_ok=True)
                
                for f in files:
                    s_file = os.path.join(root, f)
                    d_file = os.path.join(target_dir, f)
                    tasks.append((s_file, d_file, file_counter))
                    file_counter += 1

            # Use thread pool for multi-file copy
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._copy_file_internal, *t) for t in tasks]
                results = [f.result() for f in futures]
                success = all(results)

        print("\n" + "-" * 60)
        if success:
            total_time = time.time() - self.start_time
            print(f"Successfully completed in {total_time:.2f} seconds.")
            return True
        else:
            print(f"One or more errors occurred during the copy process.")
            return False

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python fast_copy.py <source> <destination>")
        sys.exit(1)

    fc = FastCopy(max_workers=8) # Default to 8 workers for directory copy
    fc.copy(sys.argv[1], sys.argv[2])
