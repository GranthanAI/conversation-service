# --- Build Stage ---
FROM python:3.12-slim AS builder

WORKDIR /build

# Install system compilation packages if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libev-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install dependencies into virtualenv
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir -r requirements.txt

# --- Final Production Stage ---
FROM python:3.12-slim AS runner

WORKDIR /app

# Install runtime event libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    libev4 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Copy project source files
COPY app/ ./app/

EXPOSE 8000

# Run entrypoint target main
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
