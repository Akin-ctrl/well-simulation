FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run as a non-root user rather than root.
RUN useradd --create-home --uid 10001 telemetry \
    && chown -R telemetry:telemetry /app
USER telemetry

# Each service serves /health, /ready, and /metrics on its own port, set by
# SERVICE_PORT in docker-compose.yml.
HEALTHCHECK --interval=15s --timeout=3s --start-period=25s --retries=3 \
    CMD python -c "import os,urllib.request,sys; \
url=f\"http://127.0.0.1:{os.environ.get('SERVICE_PORT','9101')}/ready\"; \
sys.exit(0 if urllib.request.urlopen(url, timeout=2).status == 200 else 1)"
