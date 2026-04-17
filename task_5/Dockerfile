FROM apache/airflow:2.7.2
USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-databricks==6.7.0 \
    databricks-sdk \
    databricks-sql-connector \
    faker==24.3.0
