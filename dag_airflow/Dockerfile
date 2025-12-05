FROM apache/airflow:2.10.2-python3.12

USER root
RUN apt-get update && apt-get install -y curl vim && rm -rf /var/lib/apt/lists/*

USER airflow

# install dbt + duckdb
RUN pip install \
    dbt-core \
    dbt-duckdb \
    duckdb
