# docker/Dockerfile
ARG AIRFLOW_IMAGE=apache/airflow:2.6.3-python3.10
FROM ${AIRFLOW_IMAGE}

USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        g++ \
        curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create directories
RUN mkdir -p /opt/airflow/scripts /opt/airflow/logs /opt/airflow/plugins \
    && chown -R airflow: /opt/airflow/scripts /opt/airflow/logs /opt/airflow/plugins

USER airflow

# Copy requirements
COPY requirements.txt /tmp/requirements.txt

# Install packages with compatible setuptools version
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir "setuptools>=66.0.0,<67.0.0" wheel \
    && pip install --no-cache-dir -r /tmp/requirements.txt

# Verify installations
RUN python -c "import yfinance as yf; print(f'yfinance installed successfully')" \
    && python -c "import requests; print('requests installed successfully')" \
    && python -c "import psycopg2; print('psycopg2 installed successfully')"

# Set Python path
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/scripts"