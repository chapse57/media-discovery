# Client side of the change-capture run (discovery/cdc.py): seed, change generator, consumer, checks.
FROM python:3.12-slim
RUN pip install --no-cache-dir "confluent-kafka==2.15.1" "psycopg[binary]==3.2.3"
ENV PYTHONUNBUFFERED=1 PYTHONUTF8=1
WORKDIR /work
