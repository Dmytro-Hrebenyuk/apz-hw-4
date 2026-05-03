FROM python:3.11-slim
WORKDIR /app
RUN pip install fastapi uvicorn hazelcast-python-client httpx
COPY main.py .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8081"]