"""Create text chunks from parsed CSV."""
from __future__ import annotations
import os, json, hashlib
import pandas as pd

PDF_DIR = "/opt/airflow/pdf_data"
PARSED_DIR = os.path.join(PDF_DIR, "parsed")
PARSED_CSV = os.path.join(PARSED_DIR, "parsed.csv")
CHUNKS_CSV = os.path.join(PARSED_DIR, "chunks.csv")

def _doc_id(source_file: str) -> str:
    return hashlib.sha256(source_file.encode("utf-8")).hexdigest()[:16]

def _chunk(text: str, max_len: int = 1000):
    i = 0; n = 0
    while i < len(text):
        yield n, text[i:i+max_len]
        i += max_len; n += 1

def make_chunks(parsed_csv: str = PARSED_CSV, out_csv: str = CHUNKS_CSV) -> str:
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    if not os.path.isfile(parsed_csv):
        raise FileNotFoundError(parsed_csv)
    df = pd.read_csv(parsed_csv)
    text_cols = [c for c in df.columns if c.startswith("col_")]
    df["line"] = df[text_cols].fillna("").astype(str).agg(" ".join, axis=1).str.strip()

    chunks = []
    for (src, page), g in df.groupby(["source_file", "page"], dropna=False):
        content = "\n".join(g.sort_values("row_index")["line"].tolist()).strip()
        if not content:
            continue
        doc = _doc_id(src)
        for idx, piece in _chunk(content):
            meta = {"source_file": src, "page": int(page), "chunk_index": int(idx)}
            chunks.append({
                "doc_id": doc,
                "chunk_id": f"{doc}:{page}:{idx}",
                "text": piece,
                "meta_json": json.dumps(meta),
            })
    out = pd.DataFrame(chunks, columns=["doc_id","chunk_id","text","meta_json"])
    out.to_csv(out_csv, index=False)
    return out_csv

if __name__ == "__main__":
    print(make_chunks())
