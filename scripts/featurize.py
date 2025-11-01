"""Compute embeddings for chunks and upsert to Snowflake."""
from __future__ import annotations
import os, json
import pandas as pd
from sentence_transformers import SentenceTransformer
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

PDF_DIR = "/opt/airflow/pdf_data"
PARSED_DIR = os.path.join(PDF_DIR, "parsed")
CHUNKS_CSV = os.path.join(PARSED_DIR, "chunks.csv")
EMBED_MODEL = os.getenv("EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

def embed_and_upsert(chunks_csv: str = CHUNKS_CSV):
    if not os.path.isfile(chunks_csv):
        raise FileNotFoundError(chunks_csv)
    df = pd.read_csv(chunks_csv)
    if df.empty:
        print("No chunks to embed.")
        return

    model = SentenceTransformer(EMBED_MODEL)
    vecs = model.encode(df["text"].tolist(), convert_to_numpy=True, normalize_embeddings=True)
    df["embedding_json"] = [json.dumps(v.tolist()) for v in vecs]

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn(); cs = conn.cursor()
    try:
        cs.execute("""

            CREATE TABLE IF NOT EXISTS DOC_CHUNKS (

                DOC_ID STRING,

                CHUNK_ID STRING,

                TEXT STRING,

                EMBEDDING VARIANT,

                META VARIANT

            )

        """)

        insert_sql = "INSERT INTO DOC_CHUNKS (DOC_ID, CHUNK_ID, TEXT, EMBEDDING, META) VALUES (%s,%s,%s,PARSE_JSON(%s),PARSE_JSON(%s))"

        rows = list(zip(

            df["doc_id"].tolist(),

            df["chunk_id"].tolist(),

            df["text"].tolist(),

            df["embedding_json"].tolist(),

            df["meta_json"].tolist(),

        ))

        cs.executemany(insert_sql, rows)

        conn.commit()

    finally:

        cs.close(); conn.close()


if __name__ == "__main__":

    embed_and_upsert()

