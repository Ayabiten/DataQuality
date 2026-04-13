import os
import sys

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from excel_quality_audit import ExcelQualityAudit
from dq_logging import DataQualityLogger

def main():
    test_file = os.path.join("..", "test_files", "quality_test.xlsx")
    
    if not os.path.exists(test_file):
        print(f"Test file {test_file} not found.")
        return

    print(f"--- Starting Excel Audit for {test_file} ---")
    logger = DataQualityLogger(log_name="VerifyExcel", log_dir="../test_logs")
    audit = ExcelQualityAudit(test_file, logger=logger)
    
    try:
        report = audit.execute_workbook_audit()
        meta = report["metadata"]
        print(f"\nWorkbook: {meta['file_name']}")
        print(f"Sheets Found: {meta['sheet_names']}")
        
        for sheet in report["sheets"]:
            print(f"\n--- Sheet: {sheet.sheet_name} ---")
            print(f"Rows: {sheet.total_rows} | Cols: {sheet.total_cols}")
            
            if sheet.placeholders:
                print(f"Placeholders: {sheet.placeholders}")
            
            # Print first 2 column metrics
            for metric in sheet.quality_metrics[:2]:
                print(f"  Col: {metric['column']} | Nulls: {metric['null_count']} ({metric['null_percentage']})")
                
        logger.finalize_log()
        print("\nAudit complete.")

    except Exception as e:
        print(f"Audit failed: {e}")

if __name__ == "__main__":
    main()
