# Test Files

This directory contains a variety of sample files used to test and validate the data quality audit engines.

## File Descriptions

- **`unquoted_multiline.csv`**: A "messy" CSV file with unquoted cells containing newlines. Used to test the `RobustCSVReader`.
- **`structural_issues.csv`**: Contains rows with inconsistent column counts (missing and extra columns).
- **`type_mismatch.csv`**: Contains data where the content does not match the inferred column type.
- **`missing_values.csv`**: Used to test null detection and completeness scoring.
- **`multiline.csv`**: A properly quoted CSV file with multiline cells.
- **`encoding_issue.csv`**: A file with non-standard encoding to test detection capabilities.
- **`quality_test.xlsx`**: Sample Excel file for the Excel audit engine.
- **`products_clean.json` / `sensor_data.json`**: Sample JSON files for the JSON audit engine.
