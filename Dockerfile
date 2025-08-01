FROM quay.io/astronomer/astro-runtime:12.7.0
USER root
RUN pip install --upgrade pip
RUN pip install yfinance --upgrade --no-cache-dir
RUN pip install apache-airflow-providers-postgres
RUN pip install apache-airflow-providers-http
RUN pip install --no-cache-dir dbt-postgres
RUN apt-get update && apt-get install -y git
ENV DBT_PROFILES_DIR=/usr/local/airflow/carteira_dbt_airflow/.dbt
USER astro
