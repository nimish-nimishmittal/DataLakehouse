FROM apache/airflow:2.7.1-python3.11

# Switch to root to install system packages
USER root

RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    libtesseract-dev \
    libgl1 \
    libglib2.0-0 \
    libmagic-dev \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

COPY python-etl/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN pip install --no-cache-dir psycopg2-binary minio