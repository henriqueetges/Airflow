from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime
import pandas as pd

@dag(schedule="@weekly", start_date=datetime(2024, 1, 1), catchup=False)
def fetch_multiple_tickers():

    http_sensor_task = HttpSensor(
        task_id = "check_api",
        http_conn_id="brapi",
        endpoint="quote/BBAS3",
        poke_interval=5, 
        timeout=20,
        mode="poke"
    )

    db_sensor_task = PostgresOperator(
        task_id="test_postgres_connection",
        postgres_conn_id="local_pg", 
        sql="SELECT 1;")
    
    truncate_stg = PostgresOperator(
        task_id="truncate_history_stg",
        postgres_conn_id="local_pg_stg", 
        sql="TRUNCATE TABLE inv_stg.public.stg_stock_quotes_history RESTART IDENTITY CASCADE;"
    )
    
    delete_week_data = PostgresOperator(
        task_id="delete7days",
        postgres_conn_id="local_pg", 
        sql="DELETE FROM inv.public.stock_quotes_history WHERE date >= now() - INTERVAL '7 DAYS'"
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
        final_df['date'] = pd.to_datetime(final_df['date'], unit='s')
        final_df['loaded_at'] = datetime.now()
        return pd.DataFrame(final_df)
    
    @task
    def push_to_stg(df):
        hook = PostgresHook(postgres_conn_id='local_pg_stg')
        table = 'inv_stg.public.stg_stock_quotes_history'
        columns = [c for c in df.columns]
        data_tuple = [tuple(x) for x in df[columns].values]
        hook.insert_rows(table=table, rows=data_tuple, target_fields=columns) 

    @task
    def insert_into_prod():
        prod = PostgresHook(postgres_conn_id='local_pg')
        stg = PostgresHook(postgres_conn_id='local_pg_stg')
        query = f"SELECT * FROM inv_stg.public.stg_stock_quotes_history WHERE date >= now() - INTERVAL '7 DAYS'"
        results = stg.get_pandas_df(query)
        results['date'] = results['date'].astype(str)
        results['loaded_at'] = results['loaded_at'].astype(str)
        columns = [c for c in results.columns]
        tuples = [tuple(x) for x in results[columns].values]
        prod.insert_rows(table='inv.public.stock_quotes_history', rows=tuples, target_fields=columns)
        return tuples
        
    
    tickers = fetch_list_of_tickers()
    fetch_tasks = fetch_data_from_api.expand(ticker=tickers)   
    aggregated = aggregate_results(fetch_tasks)
    insert_stg = push_to_stg(aggregated)
    insert_prod = insert_into_prod()

    http_sensor_task >> tickers >> fetch_tasks >> aggregated >> truncate_stg >> insert_stg >> delete_week_data >> insert_prod
    db_sensor_task >> tickers  >> fetch_tasks >> aggregated >> truncate_stg >> insert_stg >> delete_week_data >> insert_prod
    

fetch_multiple_tickers()
    
