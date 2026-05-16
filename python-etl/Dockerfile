FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    curl \
    tesseract-ocr \
    libtesseract-dev \
    libgl1 \
    libglib2.0-0 \
    libmagic1 \
    file \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# ── Pre-download Moondream2 weights at build time ──────────────────────────
# Bakes the ~1.86GB model into the image so the first image processed at
# runtime doesn't stall waiting for a download.
# The weights land in HuggingFace's default cache: /root/.cache/huggingface
# Remove this RUN step if you prefer a smaller image + lazy loading.
RUN python3 -c "\
import moondream as md; \
print('Pre-loading Moondream2 weights...'); \
md.vl(model='vikhyam/moondream2'); \
print('Done.')" || \
echo "WARNING: Moondream pre-download failed — will retry at runtime"

COPY . .

ENV FLASK_APP=uploader_app.py
CMD ["python", "-u", "uploader_app.py"]