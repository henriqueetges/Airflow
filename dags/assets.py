from airflow.decorators import task, dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime
import pandas as pd


@dag(
  schedule="@weekly"
    , start_date=datetime.now()
    , catchup=False
    , tags=['api']
    , description="This dag fetches asset's data available at Brapi"
)
def fetch_assets():
    """
    Fetches asset's data
    """
    http_sensor_task = HttpSensor(
        task_id = "check_api"
        , http_conn_id="brapi"
        , endpoint="quote/BBAS3"
        , poke_interval=5
        , timeout=20
        , mode="poke"
    )

    db_sensor_task = SQLExecuteQueryOperator(
        task_id="test_postgres_connection"
        , conn_id="local_pg"
        , sql="SELECT 1;"
        )
    
    @task
    def fetch_data_from_api():
        """
        Fetch asset list
        """
        endpoint = f"quote/list/"
        http_hook = HttpHook(method='GET', http_conn_id='brapi')
        response = http_hook.run(endpoint=endpoint).json().get('stocks')
        return pd.DataFrame(response)   
    
    
    @task
    def insert(data):
        """
        Args:
            data (_type_): inserts the assets data into a raw table in the production environment
        """
        table = "assets_raw"
        hook = PostgresHook(postgres_conn_id='local_pg')
        engine = hook.get_sqlalchemy_engine()
        df = pd.DataFrame(data)
        try: 
            df.to_sql(table, con=engine, if_exists='replace', index=True)
            print(f"{table} created succesfully!")
        except Exception as e:
            print(e)
            
    assets = fetch_data_from_api()   
    table = insert(assets)
    http_sensor_task >> db_sensor_task >> assets >> table

fetch_assets()
    
