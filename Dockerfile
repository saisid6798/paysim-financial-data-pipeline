FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /app

RUN apt-get update \
    && apt-get install \
        --yes \
        --no-install-recommends \
        openjdk-21-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN python -m pip install \
    --no-cache-dir \
    --upgrade pip \
    && python -m pip install \
        --no-cache-dir \
        -r requirements.txt

COPY src/ ./src/

RUN mkdir -p \
    /app/data/raw \
    /app/data/bronze \
    /app/data/silver \
    /app/data/gold/summary_exports \
    /app/data/gold/pipeline_audit \
    /app/logs \
    /app/spark-temp

ENTRYPOINT ["python", "-m", "paysim_pipeline.main"]

CMD ["--help"]