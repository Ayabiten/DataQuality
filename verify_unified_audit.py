import subprocess
import os
import sys

def run_command(cmd):
    print(f"\nExecuting: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print("Success")
        print(result.stdout)
    else:
        print("Failed")
        print(result.stderr)
    return result.returncode

def main():
    print("Starting Unified Audit Verification...")
    
    # Test 1: CSV Audit with Visualizations
    csv_path = "test_files/unquoted_multiline.csv"
    run_command([sys.executable, "main.py", csv_path, "--visualize"])
    
    # Test 2: JSON Audit
    json_path = "test_files/products_clean.json"
    run_command([sys.executable, "main.py", json_path, "--visualize"])
    
    # Test 3: Excel Audit
    excel_path = "test_files/quality_test.xlsx"
    run_command([sys.executable, "main.py", excel_path, "--visualize"])
    
    # Check outputs
    report_dir = "audit_reports"
    if os.path.exists(report_dir):
        print(f"\nChecking generated artifacts in {report_dir}:")
        files = os.listdir(report_dir)
        for f in files:
            print(f"  - {f}")
    else:
        print("\nError: No report directory found.")

if __name__ == "__main__":
    main()
