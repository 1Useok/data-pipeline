FROM apache/airflow:2.10.4-python3.12

USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
