# docker/Dockerfile
ARG AIRFLOW_IMAGE=apache/airflow:2.6.3
FROM ${AIRFLOW_IMAGE}

# Copy requirements into image (build context is project root)
COPY requirements.txt /tmp/requirements.txt

# Ensure the file is owned by the airflow user then switch to airflow user
USER root
RUN chown airflow: /tmp/requirements.txt && chmod 644 /tmp/requirements.txt

USER airflow

# Upgrade pip first to avoid potential issues
RUN pip install --no-cache-dir --upgrade pip

# Install Python deps as the airflow user (allowed by image)
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create scripts folder and ensure permissions
USER root
RUN mkdir -p /opt/airflow/scripts && chown -R airflow: /opt/airflow/scripts

# Create logs directory if it doesn't exist
RUN mkdir -p /opt/airflow/logs && chown -R airflow: /opt/airflow/logs

USER airflow

# Set Python path to include scripts directory
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/scripts"