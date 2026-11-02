FROM apache/airflow:2.10.3-python3.11

# Ingestion deps live in Airflow's own environment
RUN pip install --no-cache-dir pandas

# dbt gets its own virtualenv so its dependencies never clash with Airflow's
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.8.8 dbt-postgres==1.8.2
