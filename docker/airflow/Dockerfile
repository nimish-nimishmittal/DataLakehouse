FROM apache/airflow:2.7.1-python3.11

USER root

RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    libtesseract-dev \
    libgl1 \
    libglib2.0-0 \
    libmagic1 \
    libmagic-dev \
    file \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir "numpy==1.26.4"

COPY python-etl/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN pip install --no-cache-dir --force-reinstall \
    "numpy==1.26.4" \
    "pandas==2.1.4" \
    "pyarrow==14.0.2"

RUN pip install --no-cache-dir \
    psycopg2-binary \
    minio

RUN pip uninstall -y apache-airflow-providers-snowflake snowflake-connector-python || true