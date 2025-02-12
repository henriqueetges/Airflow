from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime
import pandas as pd

@dag(
    schedule="@monthly"
    , start_date=datetime.now()
    , catchup=False
    , tags=['api']
)
def fetch_stock_info():

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
        sql="SELECT 1;"
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
        response = http_hook.run(endpoint=endpoint).json()
        records = [{**d, 'ticker': ticker} for d in response.get('results')]
        return records
    
    @task
    def insert_into_prod(data):
        table = 'stock_info'
        hook = PostgresHook(postgres_conn_id='local_pg')
        engine = hook.get_sqlalchemy_engine()
        df = pd.DataFrame(data)
        try: 
            df.to_sql(table, con=engine, if_exists='replace', index=True)
            print(f"{table} created succesfully!")
        except Exception as e:
            print(e)

    
    @task
    def aggregate_info(data):
        df = [pd.DataFrame(df) for df in data]
        final_df = pd.concat(df, ignore_index=True)
        return final_df

    tickers = fetch_list_of_tickers()
    data = fetch_data_from_api.expand(ticker=tickers)
    aggregate = aggregate_info(data=data)
    insert_prod = insert_into_prod(data=aggregate)
        
    http_sensor_task >> db_sensor_task >> tickers >> data >> aggregate >> insert_prod


fetch_stock_info()
    
