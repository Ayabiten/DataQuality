import os
import sys

# Add parent directory to sys.path to allow importing dq_logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from csv_quality_audit import CSVQualityAudit
from dq_logging import DataQualityLogger

def main():
    # Adjust path to test_files as they are in the root's parent (DataQuality/test_files)
    test_dir = os.path.join("", "test_files")
    test_file = os.path.join(test_dir, "structural_issues.csv")
    
    if not os.path.exists(test_file):
        print(f"Test file {test_file} not found. Running from: {os.getcwd()}")
        return

    print(f"--- Starting Audit for {test_file} ---")
    
    # Initialize logger
    logger = DataQualityLogger(log_name="VerifyCSV", log_dir="../test_logs")
    
    # Initialize Audit Engine
    audit = CSVQualityAudit(test_file, logger=logger)
    
    # Run Audit
    try:
        report_data = audit.execute_full_quality_audit()
        summary = report_data['summary']
        columns = report_data['columns']
        alerts = report_data['alerts']
        
        # Display Summary
        print("\n[AUDIT SUMMARY]")
        print(f"File: {summary.file_name}")
        print(f"Rows: {summary.total_rows} | Cols: {summary.total_cols}")
        print(f"Mismatched Rows: {summary.mismatched_rows}")
        print(f"Duplicate Rows: {summary.duplicate_rows}")
        print(f"Quality Score: {summary.quality_score}%")
        
        # Display Column Profiles
        print("\n[COLUMN PROFILES]")
        for col in columns:
            print(f"Column: {col.name}")
            print(f"  Type: {col.detected_type} ({col.pandas_type})")
            print(f"  Format: {col.detected_format}")
            print(f"  Nulls: {col.null_count} ({col.null_percentage}%)")
            print(f"  Top 5 Values: {col.top_values}")
            print(f"  Unique Sample (First 5/20): {col.unique_values_sample[:5]}")
            if col.outlier_count > 0:
                print(f"  Outliers: {col.outlier_count}")
            if col.is_boolean_candidate:
                print(f"  ! Boolean Candidate Detected")
            if col.is_redundant:
                print(f"  ! Redundant/Constant Column Detected")
        
        # Check alerts
        if alerts:
            print("\n[ALERTS]")
            for a in alerts:
                print(f"  ! {a}")
                
        # --- NEW: Export to Excel ---
        report_filename = f"{summary.file_name}_audit_report.xlsx"
        report_path = os.path.join(".", report_filename)
        audit.export_report_to_excel(report_path)
        print(f"\n[REPORT EXPORTED]")
        print(f"Excel Report: {os.path.abspath(report_path)}")

        # Finalize Logs
        log_path = logger.finalize_log()
        print(f"\nAudit complete. Logs saved to: {log_path}")

    except Exception as e:
        print(f"Audit failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
