from __future__ import annotations
import os
import csv
import logging
from typing import List, Tuple

import pandas as pd

PDF_DIR = "/opt/airflow/pdf_data"
PARSED_DIR = os.path.join(PDF_DIR, "parsed")
PARSED_CSV = os.path.join(PARSED_DIR, "parsed.csv")
MAX_COLS = 10

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("extractor")

def _sanitize_row(row: List[str]) -> List[str]:
    row = ["" if v is None else str(v).strip() for v in row]
    row = row[:MAX_COLS]
    if len(row) < MAX_COLS:
        row += [""] * (MAX_COLS - len(row))
    return row

def parse_with_pdfplumber(pdf_path: str) -> List[Tuple[int, pd.DataFrame]]:
    import pdfplumber
    frames: List[Tuple[int, pd.DataFrame]] = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables() or []
            for t in tables:
                df = pd.DataFrame(t)
                if df.empty:
                    continue
                df = df.dropna(how="all")
                frames.append((i, df))
    return frames

def parse_with_tabula(pdf_path: str) -> List[Tuple[int, pd.DataFrame]]:
    try:
        import tabula  # requires Java
    except Exception as e:
        logger.warning("tabula-py not available or Java missing: %s", e)
        return []
    try:
        dfs = tabula.read_pdf(pdf_path, pages="all", multiple_tables=True, lattice=True)
        return list(enumerate(dfs, start=1))
    except Exception as e:
        logger.warning("tabula failed on %s: %s", pdf_path, e)
        return []

def parse_file(pdf_file: str):
    path = os.path.join(PDF_DIR, pdf_file)
    records = []

    frames = parse_with_pdfplumber(path)
    if not frames:
        logger.info("pdfplumber found no tables in %s; trying tabula...", pdf_file)
        frames = parse_with_tabula(path)

    if not frames:
        logger.info("No tables detected in %s", pdf_file)
        return records

    for page_num, df in frames:
        df = df.fillna("")
        for row_idx, row in enumerate(df.values.tolist()):
            cleaned = _sanitize_row(row)
            rec = {
                "source_file": pdf_file,
                "page": page_num,
                "row_index": row_idx,
            }
            for i, val in enumerate(cleaned):
                rec[f"col_{i}"] = val
            records.append(rec)

    return records

def parse_pdfs_to_csv(pdf_dir: str = PDF_DIR, out_csv: str = PARSED_CSV) -> str:
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    pdfs = [f for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf")]
    all_rows = []
    for pdf in pdfs:
        logging.info("Parsing %s", pdf)
        all_rows.extend(parse_file(pdf))

    fieldnames = ["source_file", "page", "row_index"] + [f"col_{i}" for i in range(MAX_COLS)]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    logging.info("Wrote %d rows to %s", len(all_rows), out_csv)
    return out_csv

if __name__ == "__main__":
    parse_pdfs_to_csv()
