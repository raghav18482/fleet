# python:3.12-slim is multi-arch. Deliberately not pinned to linux/amd64: compose builds
# from source, so this resolves natively on arm64 here and natively on the x86_64 Linux
# evaluation machine. Pinning would force emulation on one of the two for no benefit.
FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
