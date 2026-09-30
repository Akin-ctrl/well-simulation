FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./twin_core

RUN useradd --create-home --uid 10002 twin \
    && chown -R twin:twin /app
USER twin

HEALTHCHECK --interval=15s --timeout=3s --start-period=25s --retries=3 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/ready', timeout=2).status == 200 else 1)"

CMD ["python", "-m", "twin_core.main"]
