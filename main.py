import argparse
import os
import sys
from typing import Optional

# Ensure project directories are in sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from csv_data_quality.csv_quality_audit import CSVQualityAudit
from excel_data_quality.excel_quality_audit import ExcelQualityAudit
from json_data_quality.json_quality_audit import JSONQualityAudit
from core.visualizer import DataQualityVisualizer
from dq_logging import DataQualityLogger

def main():
    parser = argparse.ArgumentParser(description="DataQuality Audit Toolkit - Unified CLI")
    parser.add_argument("file", help="Path to the file to audit (CSV, XLSX, JSON)")
    parser.add_argument("--output", "-o", default="audit_reports", help="Directory for reports and visualizations")
    parser.add_argument("--visualize", "-v", action="store_true", help="Generate quality visualization charts")
    
    args = parser.parse_args()
    
    file_path = args.file
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return

    ext = os.path.splitext(file_path)[1].lower()
    file_id = os.path.basename(file_path).replace('.', '_')
    
    print(f"Starting audit for: {file_path}")
    logger = DataQualityLogger(log_name=f"Audit_{file_id}", log_dir="logs")
    
    try:
        # 1. Select the correct Audit Model
        if ext == '.csv':
            audit = CSVQualityAudit(file_path, logger=logger)
            report = audit.execute_full_audit()
            df_for_viz = audit.data_frame
        elif ext in ['.xlsx', '.xls']:
            audit = ExcelQualityAudit(file_path, logger=logger)
            report = audit.execute_full_audit()
            # For viz, we take the first sheet if multiple exist
            first_sheet = list(report['sheets'].keys())[0]
            df_for_viz = pd.read_excel(file_path, sheet_name=first_sheet)
        elif ext == '.json':
            audit = JSONQualityAudit(file_path, logger=logger)
            report = audit.execute_full_audit()
            df_for_viz = report['raw_df']
        else:
            print(f"❌ Unsupported file extension: {ext}")
            return

        # 2. Export Excel Report
        if not os.path.exists(args.output):
            os.makedirs(args.output)
            
        report_path = os.path.join(args.output, f"{file_id}_report.xlsx")
        audit.export_report_to_excel(report_path)
        print(f"Excel Report generated: {report_path}")

        # 3. Optional Visualizations
        if args.visualize and not df_for_viz.empty:
            viz = DataQualityVisualizer(output_dir=args.output)
            print("Generating visualizations...")
            
            # Null Heatmap
            heatmap_path = viz.generate_null_heatmap(df_for_viz, file_id)
            print(f"Null Heatmap: {heatmap_path}")
            
            # Type Distribution
            # Handle different report structures (CSV/JSON vs Excel workbook)
            if 'columns' in report:
                type_path = viz.generate_type_distribution(report['columns'], file_id)
                print(f"Type Distribution: {type_path}")
            elif ext in ['.xlsx', '.xls'] and 'sheets' in report:
                # Use columns from the first audited sheet
                first_sheet = list(report['sheets'].keys())[0]
                sheet_data = report['sheets'][first_sheet]
                if 'columns' in sheet_data:
                    type_path = viz.generate_type_distribution(sheet_data['columns'], f"{file_id}_{first_sheet}")
                    print(f"Type Distribution (Sheet {first_sheet}): {type_path}")
            
            # Quality Summary
            summary = None
            if 'summary' in report:
                summary = report['summary']
            elif ext in ['.xlsx', '.xls'] and 'sheets' in report:
                first_sheet = list(report['sheets'].keys())[0]
                summary = report['sheets'][first_sheet].get('summary')

            if summary and hasattr(summary, 'quality_score'):
                metrics = {
                    'Quality Score': summary.quality_score,
                    'Completeness': getattr(summary, 'completeness_score', 100.0)
                }
                summary_path = viz.generate_quality_summary_chart(metrics, file_id)
                print(f"Quality Summary: {summary_path}")

        print("\nAudit Completed Successfully!")

    except Exception as e:
        print(f"Audit Failed: {e}")
        logger.log_exception(file_path, "AuditFatalError")

if __name__ == "__main__":
    # Import pandas only if needed for Excel handling in the main selector
    import pandas as pd 
    main()
