# Use official Airflow image
FROM apache/airflow:2.9.2-python3.11

USER root
# Install Java 17 for tabula-py and tesseract OCR for scans
RUN apt-get update && apt-get install -y --no-install-recommends         openjdk-17-jre-headless         tesseract-ocr         libglib2.0-0 libsm6 libxrender1 libxext6         ghostscript         && rm -rf /var/lib/apt/lists/*

USER airflow
# Copy requirements and install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Create expected directories
RUN mkdir -p /opt/airflow/pdf_data /opt/airflow/scripts /opt/airflow/models

# Copy project files (dags & scripts are mounted too; copy for image builds)
COPY dags /opt/airflow/dags
COPY scripts /opt/airflow/scripts
