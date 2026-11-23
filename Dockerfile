FROM apache/airflow:3.4.0
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" oracledb requests
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core dbt-oracle
