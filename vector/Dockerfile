# Embedding + pgvector client for discovery/vectors.py. CPU only, model runs locally.
FROM python:3.11-slim
RUN pip install --no-cache-dir torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir "sentence-transformers==3.3.1" "transformers==4.46.3" \
        "psycopg[binary]==3.2.3" "pgvector==0.3.6" "numpy<2.2"
ENV HF_HOME=/hf-cache PYTHONUNBUFFERED=1
WORKDIR /work
