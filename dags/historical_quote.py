from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime
import pandas as pd

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
    def fetch_list_of_tickers():
        hook = PostgresHook(postgres_conn_id='local_pg')
        sql = 'SELECT DISTINCT ticker from inv.public.transac'
        results = hook.get_records(sql)
        tickers = [row[0] for row in results]
        return tickers

    @task
    def fetch_data_from_api(ticker):
        http_hook = HttpHook(method='GET', http_conn_id='brapi')
        endpoint = f"quote/{ticker}"
        params = {
            "range": "3mo",
            "interval": "1d",
        }
        response = http_hook.run(endpoint=endpoint, data=params).json()
        historical_records = response.get('results')[0].get('historicalDataPrice')
        historical_records = [{**d, 'ticker': ticker} for d in historical_records]
        return historical_records
    
    @task
    def aggregate_results(results):
        dfs = [pd.DataFrame(result) for result in results]
        final_df = pd.concat(dfs, ignore_index=True)
        final_df['date'] = pd.to_datetime(final_df['date'])
        final_df['loaded_at'] = datetime.now()
        return pd.DataFrame(final_df)
    
    @task
    def push_to_stg(df):
        hook = PostgresHook(postgres_conn_id='local_pg_stg')
        table = ' inv_stg.public.stock_quotes_history'
        columns = [c for c in df.columns]
        data_tuple = [tuple(x) for x in df[columns].values]
        query = f"""
                INSERT INTO {table} ({[x for x in columns]})
                VALUES %s
                """
        hook.insert_rows(table=table, rows=data_tuple, target_fields=columns)
    
    tickers = fetch_list_of_tickers()
    fetch_tasks = fetch_data_from_api.expand(ticker=tickers)   
    aggregated = aggregate_results(fetch_tasks)
    insert_task = push_to_stg(aggregated)

    http_sensor_task >> tickers >> fetch_tasks >> aggregated >> insert_task

fetch_multiple_tickers()
    
