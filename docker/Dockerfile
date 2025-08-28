# docker/Dockerfile
ARG AIRFLOW_IMAGE=apache/airflow:2.6.3
FROM ${AIRFLOW_IMAGE}

# copy requirements into image (build context is project root)
COPY requirements.txt /tmp/requirements.txt

# ensure the file is owned by the airflow user then switch to airflow user
USER root
RUN chown airflow: /tmp/requirements.txt && chmod 644 /tmp/requirements.txt

USER airflow
# Install Python deps as the airflow user (allowed by image)
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# create scripts folder and ensure permissions
USER root
RUN mkdir -p /opt/airflow/scripts && chown -R airflow: /opt/airflow/scripts
USER airflow
