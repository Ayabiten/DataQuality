import os
import sys

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from json_quality_audit import JSONQualityAudit
from dq_logging import DataQualityLogger

def main():
    test_file = os.path.join("..", "test_files", "events_messy.json")
    
    if not os.path.exists(test_file):
        print(f"Test file {test_file} not found.")
        return

    print(f"--- Starting JSON Audit for {test_file} ---")
    logger = DataQualityLogger(log_name="VerifyJSON", log_dir="../test_logs")
    audit = JSONQualityAudit(test_file, logger=logger)
    
    try:
        profile = audit.execute_full_json_audit()
        print(f"\nFile: {profile.file_name}")
        print(f"Max Nesting Depth: {profile.max_depth}")
        print(f"Structure: {profile.structure_type} ({profile.item_count} items)")
        
        if profile.key_inconsistencies:
            print(f"Key Inconsistencies Detected: Yes")
            
        logger.finalize_log()
        print("\nAudit complete.")

    except Exception as e:
        print(f"Audit failed: {e}")

if __name__ == "__main__":
    main()
