from __future__ import annotations
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

PDF_DIR = "/opt/airflow/pdf_data"
PARSED_DIR = os.path.join(PDF_DIR, "parsed")
PARSED_CSV = os.path.join(PARSED_DIR, "parsed.csv")
CHUNKS_CSV = os.path.join(PARSED_DIR, "chunks.csv")
TARGET_TABLE = "PDF_TABLE"  # change if needed

ENABLE_STAGE_UPLOAD = str(os.getenv("ENABLE_STAGE_UPLOAD", "false")).lower() in {"1","true","yes"}
STAGE_NAME = os.getenv("STAGE_NAME", "PDF_STAGE")

def ensure_dirs():
    os.makedirs(PARSED_DIR, exist_ok=True)

def extract_callable(**context):
    ensure_dirs()
    from scripts.extractor import parse_pdfs_to_csv
    parse_pdfs_to_csv(pdf_dir=PDF_DIR, out_csv=PARSED_CSV)

def load_csv_to_snowflake(**context):
    import pandas as pd
    if not os.path.isfile(PARSED_CSV):
        raise FileNotFoundError(f"Missing parsed CSV at {PARSED_CSV}")
    df = pd.read_csv(PARSED_CSV).fillna("")
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cs = conn.cursor()
    try:
        placeholders = ",".join(["%s"] * len(df.columns))
        columns = ",".join([f'"{c.upper()}"' for c in df.columns])
        insert_sql = f"INSERT INTO {TARGET_TABLE} ({columns}) VALUES ({placeholders})"
        cs.executemany(insert_sql, df.values.tolist())
        conn.commit()
    finally:
        cs.close(); conn.close()

def make_chunks_callable(**context):
    from scripts.make_chunks import make_chunks
    make_chunks(parsed_csv=PARSED_CSV, out_csv=CHUNKS_CSV)

def embed_and_upsert_callable(**context):
    from scripts.featurize import embed_and_upsert
    embed_and_upsert(chunks_csv=CHUNKS_CSV)

def stage_pdfs_callable(**context):
    if not ENABLE_STAGE_UPLOAD:
        print("Stage upload disabled.")
        return
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn(); cs = conn.cursor()
    try:
        cs.execute(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME} DIRECTORY = (ENABLE = TRUE)")
        # Upload PDFs from local folder into the stage
        cs.execute(f"PUT file://{PDF_DIR}/*.pdf @{STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        cs.execute(f"ALTER STAGE {STAGE_NAME} REFRESH")
        conn.commit()
    finally:
        cs.close(); conn.close()

def get_create_table_sql():
    cols = [
        '"SOURCE_FILE" VARCHAR',
        '"PAGE" INTEGER',
        '"ROW_INDEX" INTEGER',
        '"COL_0" VARCHAR',
        '"COL_1" VARCHAR',
        '"COL_2" VARCHAR',
        '"COL_3" VARCHAR',
        '"COL_4" VARCHAR',
        '"COL_5" VARCHAR',
        '"COL_6" VARCHAR',
        '"COL_7" VARCHAR',
        '"COL_8" VARCHAR',
        '"COL_9" VARCHAR'
    ]
    return f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (" + ",".join(cols) + ")"

with DAG(
    dag_id="pdf_to_snowflake_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # trigger manually or set a cron
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["pdf", "snowflake", "ml"],
) as dag:

    wait_for_pdfs = FileSensor(
        task_id="wait_for_pdfs",
        fs_conn_id="fs_default",
        filepath="pdf_data",
        poke_interval=15,
        timeout=60*10,
        mode="poke",
        soft_fail=True,
    )

    ensure_parsed_dir = PythonOperator(
        task_id="ensure_parsed_dir",
        python_callable=ensure_dirs,
    )

    stage_pdfs = PythonOperator(
        task_id="stage_pdfs_if_enabled",
        python_callable=stage_pdfs_callable,
    )

    extract = PythonOperator(
        task_id="extract_tables",
        python_callable=extract_callable,
    )

    create_table = SnowflakeOperator(
        task_id="create_table",
        snowflake_conn_id="snowflake_default",
        sql=get_create_table_sql(),
    )

    load_to_snowflake = PythonOperator(
        task_id="load_csv_to_snowflake",
        python_callable=load_csv_to_snowflake,
    )

    make_chunks = PythonOperator(
        task_id="make_chunks",
        python_callable=make_chunks_callable,
    )

    embed_and_upsert = PythonOperator(
        task_id="embed_and_upsert",
        python_callable=embed_and_upsert_callable,
    )

    wait_for_pdfs >> ensure_parsed_dir >> stage_pdfs >> extract >> create_table >> load_to_snowflake >> make_chunks >> embed_and_upsert
