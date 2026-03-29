from dq_logging import DataQualityLogger, FileErrorLog, RequestErrorLog
import os

def test_logging():
    # Now we set the directory at initialization
    logger = DataQualityLogger("TestAudit", log_dir="test_logs")

    # 1. Using Scenario: File Processing
    print("\n--- Scenario: File Audit ---")
    with logger.scenario_file("test_files/structural_issues.csv", error_type="StructuralAudit"):
        print("Auditing file structure...")
        pass

    # 2. Using Scenario: Request (Simulating Failure)
    print("\n--- Scenario: API Request ---")
    try:
        with logger.scenario_request("https://api.example.com/v1/data", method="POST"):
            print("Sending request...")
            raise ConnectionError("Failed to connect to host")
    except ConnectionError:
        print("Caught expected ConnectionError in test script.")

    # 3. Manual Log with Automatic Std Logging
    logger.log_file_error(
        file_path="test_files/type_mismatch.csv",
        file_type="CSV",
        error_type="Type Mismatch",
        error_message="Found string 'N/A' in numeric column 'id'",
        row=10
    )

    # 4. Finalize and Summary
    print("\n--- Final Summary ---")
    print(logger)
    log_file = logger.finalize_log()
    print(f"\nFinal log file created at: {log_file}")

if __name__ == "__main__":
    test_logging()
