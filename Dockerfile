FROM python:3.11-slim AS builder

ARG REQUIREMENTS_FILE=requirements.runtime.txt

# ── System deps ───────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps ───────────────────────────────────────────────────────────────
WORKDIR /app
COPY requirements*.txt ./
RUN pip wheel --no-cache-dir --wheel-dir /wheels -r "${REQUIREMENTS_FILE}"

FROM python:3.11-slim

ARG REQUIREMENTS_FILE=requirements.runtime.txt

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements*.txt ./
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r "${REQUIREMENTS_FILE}" \
    && rm -rf /wheels

# ── App code ──────────────────────────────────────────────────────────────────
COPY ingestion/  ingestion/
COPY batch/      batch/
COPY config/     config/
COPY serving/    serving/
COPY api/        api/
COPY ml/         ml/
COPY speed/      speed/
COPY warehouse/  warehouse/

# Tạo __init__.py cho package root nếu cần import tuyệt đối
RUN touch __init__.py

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
