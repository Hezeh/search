FROM tiangolo/uvicorn-gunicorn:python3.8-slim

RUN pip install --no-cache-dir fastapi[all] elasticsearch[async] google-cloud-pubsub firebase-admin

COPY ./app /app