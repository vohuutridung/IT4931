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
RUN pip wheel --no-cache-dir --wheel-dir /wheels \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    -r "${REQUIREMENTS_FILE}"

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

ARG REQUIREMENTS_FILE=requirements.runtime.txt

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        librdkafka1 \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# [FIX] Chạy với non-root user để giảm attack surface
RUN groupadd --gid 1001 appgroup \
    && useradd --uid 1001 --gid appgroup --create-home appuser

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
COPY shared/     shared/

# Tạo __init__.py cho package root nếu cần import tuyệt đối
RUN touch __init__.py \
    && chown -R appuser:appgroup /app

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# [FIX] Non-root user
USER appuser

# [FIX] Thêm CMD mặc định để tránh lỗi khi chạy standalone
CMD ["python", "-c", "print('social-python image ok - specify a Kubernetes command or run a module explicitly')"]
