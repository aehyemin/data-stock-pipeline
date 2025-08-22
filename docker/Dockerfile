FROM apache/airflow:2.9.3
USER airflow

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_MAJOR_MINOR=3.12
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_MINOR}.txt

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINTS_URL}"