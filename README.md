# PDF → Snowflake Pipeline (Dockerized Airflow)

Automated pipeline that extracts table data from PDF files, **optionally stages PDFs in Snowflake**, loads rows into Snowflake, and
(new) **creates text chunks + embeddings** for ML / RAG.

## What's Included
- **PDF Parsing** with Python (`pdfplumber`, optional `tabula-py` for tricky tables).
- **Airflow DAG** orchestrating:
  - (Optional) **Stage PDFs in Snowflake** via `PUT` and `DIRECTORY` refresh.
  - **Extract tables** to CSV.
  - **Chunk text** and **create embeddings**.
  - **Load parsed rows + embeddings** to Snowflake.
- **Dockerized stack**: Airflow (webserver/scheduler/worker/triggerer), Postgres (metadata), Redis (broker), all in containers.
- **ML-ready scripts**: `make_chunks.py`, `featurize.py`.

## Data Flow (Default Hybrid Option)
- PDFs are read **locally inside the Airflow container** from `./pdf_data` (mounted at `/opt/airflow/pdf_data`).
- (Optional) PDFs are **uploaded to a Snowflake stage** for governance/lineage if `ENABLE_STAGE_UPLOAD=true`.
- Tables are parsed to `/opt/airflow/pdf_data/parsed/parsed.csv`.
- Chunks are produced at `/opt/airflow/pdf_data/parsed/chunks.csv`.
- Embeddings are computed (Sentence-Transformers) and rows are upserted to Snowflake.

> You can disable stage upload by leaving `ENABLE_STAGE_UPLOAD` empty or `false` in `.env`.
> If you prefer full in-warehouse parsing using a Python UDF, ask and we’ll add that path too.

## Prerequisites
- Docker & Docker Compose
- Snowflake account & credentials (Warehouse, Database, Schema, Role)

## Quick Start
1. Clone and set env:
   ```bash
   git clone https://github.com/your-org/pdf-to-snowflake-pipeline.git
   cd pdf-to-snowflake-pipeline
   cp .env.example .env
   # Fill in Snowflake creds; toggle ENABLE_STAGE_UPLOAD if you want stage copies
   ```

2. Generate a sample PDF (or drop yours into `./pdf_data`):
   ```bash
   docker compose run --rm airflow-worker python /opt/airflow/scripts/generate_sample_pdf.py
   ```

3. Build & start:
   ```bash
   docker compose down -v
   docker compose build --pull --no-cache
   docker compose up airflow-init
   docker compose up -d
   ```

4. Open Airflow UI: http://localhost:8080 (credentials from `.env`)

5. Run the DAG `pdf_to_snowflake_dag`:
   - Unpause, then Trigger in the UI.

## Configuration
- `.env` drives both **Snowflake connection** and **feature toggles**:
  - `ENABLE_STAGE_UPLOAD=true|false` – upload PDFs to Snowflake stage before/after extraction.
  - `STAGE_NAME=PDF_STAGE` – stage name to use.
  - `EMBED_MODEL=sentence-transformers/all-MiniLM-L6-v2` – embedding model.

### Snowflake Connections
- Airflow sets `snowflake_default` using `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` constructed from `.env`.
- Target tables created:
  - `PDF_TABLE` – parsed rows (generic columns: SOURCE_FILE, PAGE, ROW_INDEX, COL_0..COL_9).
  - `DOC_CHUNKS` – text chunks and embeddings (EMBEDDING stored as VARIANT list for broad compatibility).

## Common Commands
```bash
# Logs
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker

# Run scripts ad-hoc inside the container
docker compose run --rm airflow-worker python /opt/airflow/scripts/extractor.py
docker compose run --rm airflow-worker python /opt/airflow/scripts/make_chunks.py
docker compose run --rm airflow-worker python /opt/airflow/scripts/featurize.py
```

## Notes
- Java 17 + tesseract OCR are installed in the image (Tabula & OCR support).
- For large loads, consider S3 + `COPY INTO` for the parsed CSV; embeddings are best inserted via Python hook or Snowpark.
- For stricter schemas, map `col_i` → named columns in `extractor.py` and `get_create_table_sql()`.

## Security
- Never commit real secrets. Keep `.env` private.
- Prefer keypair auth for Snowflake in production.

## License
MIT
