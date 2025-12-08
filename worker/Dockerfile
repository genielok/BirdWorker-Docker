FROM python:3.11-slim

WORKDIR /app

RUN pip install boto3

COPY src/worker.py .

CMD ["python", "-u", "worker.py"]