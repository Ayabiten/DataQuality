import csv
import pandas as pd
import io
import os
import chardet
from typing import List, Dict, Any, Optional, Tuple, Iterator

class RobustCSVReader:
    """
    A robust CSV reader designed to handle messy files, especially those with
    unquoted multiline cells and inconsistent column counts.
    Supports chunked streaming for large files.
    """

    def __init__(self, file_path: str, delimiter: Optional[str] = None, encoding: Optional[str] = None):
        self.file_path = file_path
        self.delimiter = delimiter
        self.encoding = encoding
        self.header: List[str] = []
        self.expected_cols: int = 0
        self.errors: List[Dict[str, Any]] = []
        self.healed_indices: List[int] = []

    def detect_settings(self) -> Tuple[str, str]:
        """Detects encoding and delimiter if not provided."""
        if not self.encoding:
            with open(self.file_path, 'rb') as f:
                raw = f.read(50000)
                res = chardet.detect(raw)
                self.encoding = res['encoding'] or 'utf-8'
        
        if not self.delimiter:
            try:
                with open(self.file_path, 'r', encoding=self.encoding) as f:
                    content = f.read(50000)
                    dialect = csv.Sniffer().sniff(content)
                    self.delimiter = dialect.delimiter
            except:
                self.delimiter = ','  # Fallback
        
        return self.encoding, self.delimiter

    def read_robustly(self, chunksize: Optional[int] = None) -> Any:
        """
        Reads the CSV file and heals it. Returns a DataFrame if chunksize is None,
        otherwise returns a generator of DataFrames.
        """
        if chunksize:
            return self._read_in_chunks(chunksize)
        
        # Original logic for full load
        rows = []
        for chunk in self._read_in_chunks(chunksize=None):
            rows.extend(chunk.values.tolist())
        return pd.DataFrame(rows, columns=self.header)

    def _read_in_chunks(self, chunksize: Optional[int] = None) -> Iterator[pd.DataFrame]:
        """Generator that yields normalized chunks of data."""
        enc, sep = self.detect_settings()
        
        with open(self.file_path, 'r', encoding=enc) as f:
            reader = csv.reader(f, delimiter=sep)
            
            try:
                self.header = []
                while not self.header:
                    line = next(reader)
                    self.header = [h.strip() for h in line if h is not None]
                    if not any(self.header): self.header = []
                self.expected_cols = len(self.header)
            except StopIteration:
                return

            current_row: List[str] = []
            chunk_rows = []
            row_count = 0

            for line_idx, row in enumerate(reader, 2):
                if not row: continue
                
                if not current_row:
                    current_row = row
                    continue

                # Heuristic for multiline continuation
                is_continuation = (len(row) == 1) or \
                                  (len(current_row) < self.expected_cols and (len(current_row) + len(row) - 1 <= self.expected_cols))

                if is_continuation:
                    current_row[-1] += " " + row[0]
                    if len(row) > 1: current_row.extend(row[1:])
                    self.healed_indices.append(row_count)
                else:
                    # Finalize current_row
                    chunk_rows.append(self._normalize_row(current_row))
                    row_count += 1
                    current_row = row
                    
                    if len(row) > self.expected_cols:
                        self.errors.append({'line': line_idx, 'message': 'Too many columns', 'data': row})

                # Yield chunk if size reached
                if chunksize and len(chunk_rows) >= chunksize:
                    yield pd.DataFrame(chunk_rows, columns=self.header)
                    chunk_rows = []

            # Handle last row
            if current_row:
                chunk_rows.append(self._normalize_row(current_row))
            
            if chunk_rows:
                yield pd.DataFrame(chunk_rows, columns=self.header)

    def _normalize_row(self, row: List[str]) -> List[str]:
        """Pads or truncates a row to match expected column count."""
        if len(row) < self.expected_cols:
            return row + [None] * (self.expected_cols - len(row))
        return row[:self.expected_cols]

if __name__ == "__main__":
    test_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "test_files", "unquoted_multiline.csv")
    if os.path.exists(test_path):
        reader = RobustCSVReader(test_path)
        for i, chunk in enumerate(reader.read_robustly(chunksize=2)):
            print(f"Chunk {i}:\n", chunk)
