FROM apache/airflow:3.1.7-python3.13

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      default-libmysqlclient-dev \
      pkg-config \
      build-essential \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt

RUN python -m pip install --no-cache-dir -U pip setuptools wheel && \
    python -m pip install --no-cache-dir -r /requirements.txt
