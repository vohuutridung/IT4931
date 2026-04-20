# ── Base image ────────────────────────────────────────────────────────────────
FROM python:3.11-slim

# ── System deps ───────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        librdkafka-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps ───────────────────────────────────────────────────────────────
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── App code ──────────────────────────────────────────────────────────────────
COPY ingestion/  ingestion/
COPY batch/      batch/
COPY shared/     shared/

# Tạo __init__.py cho package root nếu cần import tuyệt đối
RUN touch __init__.py

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
