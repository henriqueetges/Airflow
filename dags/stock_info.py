from airflow.decorators import task, dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime
import pandas as pd
from conda_env.cli.main_list import description


@dag(
    schedule="@monthly"
    , start_date=datetime.now()
    , catchup=False
    , tags=['api']
    , description="Pulls stock info such as industry, adresses, webpage and etc."
)
def fetch_stock_info():
    http_sensor_task = HttpSensor(
        task_id = "check_api"
        , endpoint="quote/BBAS3"
        , http_conn_id="brapi"
        , poke_interval=5
        , timeout=20
        , mode="poke"
        , description="Checks for API availability"
    )


    db_sensor_task = SQLExecuteQueryOperator(
        task_id="test_postgres_connection"
        , conn_id="local_pg"
        , sql="SELECT 1;"
        , description="Checks for database availability"
    )
    
    @task
    def fetch_list_of_tickers():
        """
        Fetches list of tickers transacted upon
        """
        hook = PostgresHook(postgres_conn_id='local_pg')
        sql = 'SELECT DISTINCT ticker from inv.public.transac'
        results = hook.get_records(sql)
        tickers = [row[0] for row in results]
        return tickers

    @task
    def fetch_data_from_api(ticker):
        """
        Triggers get request to endpoints to fetch info data for each ticker
        """
        http_hook = HttpHook(method='GET', http_conn_id='brapi')
        endpoint = f"quote/{ticker}"
        response = http_hook.run(endpoint=endpoint).json()
        records = [{**d, 'ticker': ticker} for d in response.get('results')]
        return records
    
    @task
    def insert_into_prod(data):
        """
        Inserts into PROD data for each ticker
        """
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
        """
        Aggregate all the tickers data into a single DataFrame
        """
        df = [pd.DataFrame(df) for df in data]
        final_df = pd.concat(df, ignore_index=True)
        return final_df

    tickers = fetch_list_of_tickers()
    data = fetch_data_from_api.expand(ticker=tickers)
    aggregate = aggregate_info(data=data)
    insert_prod = insert_into_prod(data=aggregate)
        
    http_sensor_task >> db_sensor_task >> tickers >> data >> aggregate >> insert_prod


fetch_stock_info()
    
