FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY akshare-adapter/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY akshare-adapter/main.py ./

EXPOSE 8002

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8002/health')" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
