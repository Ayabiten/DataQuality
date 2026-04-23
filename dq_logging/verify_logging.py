import os
import sys

# Add parent directory to sys.path to allow imports from root level
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from dq_logging.models import DataQualityLogger
from database.Model import DataModel

def run_logging_demo():
    print("=== DataQualityLogger: Manual (Long-Script) Usage Demo ===")
    
    # 1. Instantiate the logger (Classic way)
    # Good for long scripts where logging happens across multiple functions
    logger = DataQualityLogger(log_name="LongSession", log_dir="test_logs")
    
    print("\n[Phase 1] Initialization")
    logger.info("Initializing long-running audit...")
    logger.success("Environment verified.")
    
    # Simulate some work
    print("\n[Phase 2] Operation")
    logger.log_request(
        url="https://api.service.com/data",
        method="GET",
        level="INFO",
        status_code=200,
        error_message="Data stream active."
    )
    
    print("\n[Phase 3] Error Handling")
    # You can still use scenario decorators manually if needed
    try:
        with logger.scenario_file(file_path="large_dataset.csv"):
            logger.info("Processing millions of rows...")
            # Simulate a partial failure
            logger.warning("Chunk 4 contains 5 null values.")
            raise IOError("Disk timeout during write")
    except IOError:
        print("  (!) Recovering from IOError, logged to audit.")

    print("\n[Phase 4] Analytics & Finalization")
    summary = logger.get_summary()
    print(f"  Final Stats: {summary['total_general_logs']} status updates, {summary['total_file_logs']} file events.")
    
    # 2. Finalize manually at the very end
    # This ensures the session is closed and path is printed
    logger.finalize_log()

    print("\n=== Demo Completed. Check 'test_logs/LongSession.log' ===")

if __name__ == "__main__":
    run_logging_demo()
