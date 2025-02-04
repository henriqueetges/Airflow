import json
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import pandas as pd


def get_max_date():
    hook = PostgresHook(postgres_conn_id='local_pg')
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT MAX(date) from inv.public.stock_quotes_history')
            max_date = cur.fetchone()[0]
            print(f"Last record is as if {max_date}")
    return max_date or datetime.now()


def fetch_data_from_api(ti):
    max_date = ti.xcom_pull(task_ids="check_api")
    http_hook = HttpHook(method='GET', http_conn_id='brapi')
    headers = {
        "range": "3mo",
        "interval": "1d",
        }
    response = http_hook.run(endpoint="quote/BBSE3", headers=headers).json()
    #results = pd.json_normalize(response, 'results')
    print(response)

def save_to_db(ti):
    pass
    # data = ti.xcom_ull(task_ids="fetch_api_data")
    # if not data:
    #     print('No new data')
    #     return
    # hook = PostgresHook(postgres_conn_id = 'local_pg')
    # with hook.get_conn() as conn:
    #     with conn.cursor() as cur:
    #         for record in data:
    #             cur.execute('INSERT INTO inv.public.stock_quotes_history (ticker, date, open, low, high, volume, adjusted_close, loaded_at) \
    #                          VALUES (%s,  %s, %s, %s, %s, %s, %s, %s)'), (record[])
    #     conn.commit()
    #     print('Data added')

default_args = {
    "owner": "henrique",
    "start_date": datetime(2024, 2, 1),
    "retries": 0, 
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    "fetch_stock_historical_quote",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:
    
    fetch_max_date = PythonOperator(
        task_id = 'get_last_input_date',
        python_callable = get_max_date,
    )

    is_api_available = HttpSensor(
        task_id = "check_api",
        http_conn_id="brapi",
        endpoint="quote/BBAS3",
        poke_interval=5, 
        timeout=20,
    )

    fetch_from_api = PythonOperator(
        task_id = "fetch_api_data",
        python_callable=fetch_data_from_api,
    )

    save_data = PythonOperator(
        task_id="save_to_pg",
        python_callable=save_to_db,
    )

    fetch_max_date >> is_api_available >>fetch_from_api 