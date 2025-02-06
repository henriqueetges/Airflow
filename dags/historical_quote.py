import json
from airflow import DAG
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime
import pandas as pd

TICKERS = ['BBAS3', 'PETR4']

@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False)
def fetch_multiple_tickers():
    http_sensor_task = HttpSensor(
        task_id = "check_api",
        http_conn_id="brapi",
        endpoint="quote/BBAS3",
        poke_interval=5, 
        timeout=20,
        mode="poke"
    )

    @task
    def fetch_data_from_api(ticker):
        http_hook = HttpHook(method='GET', http_conn_id='brapi')
        endpoint = f"quote/{ticker}"
        params = {
            "range": "3mo",
            "interval": "1d",
        }
        response = http_hook.run(endpoint=endpoint, data=params).json()
        return response
    
    @task
    def aggregate_results(results):
        dfs = [pd.DataFrame(result) for result in results]
        final_df = pd.concat(dfs, ignore_index=True)
        return final_df.to_json()
    
    fetch_tasks = []
    for ticker in TICKERS:
        fetch_task = fetch_data_from_api.override(task_id=f"fetch {ticker} from BRAPI")(ticker)
        fetch_tasks.append(fetch_task)
    aggregated = aggregate_results(fetch_tasks)

    http_sensor_task >> fetch_tasks >> aggregated

fetch_multiple_tickers()
    
    # @task
    # def save_to_db(ti):
    #     pass
    #     # data = ti.xcom_ull(task_ids="fetch_api_data")
    #     # if not data:
    #     #     print('No new data')
    #     #     return
    #     # hook = PostgresHook(postgres_conn_id = 'local_pg')
    #     # with hook.get_conn() as conn:
    #     #     with conn.cursor() as cur:
    #     #         for record in data:
    #     #             cur.execute('INSERT INTO inv_stg.public.stock_quotes_history (ticker, date, open, low, high, volume, adjusted_close, loaded_at) \
    #     #                          VALUES (%s,  %s, %s, %s, %s, %s, %s, %s)'), (record[])
    #     #     conn.commit()
    #     #     print('Data added')

    # @task
    # def get_max_date():
    #     hook = PostgresHook(postgres_conn_id='local_pg')
    #     with hook.get_conn() as conn:
    #         with conn.cursor() as cur:
    #             cur.execute('SELECT MAX(date) from inv.public.stock_quotes_history')
    #             max_date = cur.fetchone()[0]
    #             print(f"Last record is as if {max_date}")
    #     return max_date or datetime.now()


