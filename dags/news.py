from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import yfinance
from sqlalchemy import Table, MetaData, insert

@dag(
    schedule="@daily"
    , start_date=datetime(2025, 1, 1)
    , catchup=False
    , tags=['api']

)

def fetch_news():
    """
    "Pulls news for each of the tickers inside of the wallet"
    """
    @task
    def fetch_list_of_tickers():
        """
        Fetches list of tickers transacted upon
        """
        hook = PostgresHook(postgres_conn_id='local_pg')
        sql = """select distinct stock from assets_raw where type = 'stock'"""
        results = hook.get_records(sql)
        ticker_list = [row[0]+".SA" for row in results]
        return ticker_list
    
    @task
    def fetch_news_from_api(ticker):
        """
        Fetches news from API by using yfinance for each ticker
        """
        news = yfinance.Ticker(ticker).news
        list_of_news = [x.get('content') for x in news]
        list_of_contents = [{**d, 'ticker': ticker} for d in list_of_news]
        return list_of_contents

    @task
    def aggregate_news(news):
        """
        Aggregates news from all of the stocks
        """
        df = [pd.json_normalize(df) for df in news]
        final = pd.concat(df, ignore_index=True)
        print(final['pubDate'].max())
        cols_to_keep = [
            'id'
            ,'title'
            ,'summary'
            ,'provider.displayName'
            ,'pubDate'
            ,'ticker'
            ,'clickThroughUrl.url'
        ]
        return final[cols_to_keep]

    @task
    def insert_into_stg(data):
        """
        Insert the news into stg
        """
        table = 'stg_stock_news'
        hook = PostgresHook(postgres_conn_id='local_pg_stg')
        engine = hook.get_sqlalchemy_engine()
        try:
            data.to_sql(table, con=engine, if_exists='replace', index=True)
            print(f"{table} created succesfully!")
            print(f"Last news publicated in {hook.get_records(f'SELECT max("pubDate") FROM {table} LIMIT 1')[0]}")
        except Exception as e:
            print(e)

    @task
    def insert_into_prod():
        """
        Inserts into PROD only the news that are new
        """
        prod = PostgresHook(postgres_conn_id='local_pg')
        stg = PostgresHook(postgres_conn_id='local_pg_stg')
        max_date = datetime.fromisoformat(prod.get_pandas_df('SELECT MAX("pubDate") FROM inv.public.stock_news').iloc[0,0])
        print(f"Last publicated news in prod is at {max_date}")
        query = f"""SELECT * FROM inv_stg.public.stg_stock_news  WHERE "pubDate"::timestamp > '{max_date}'      
        """
        results = stg.get_pandas_df(query)
        print(f"Found {results.shape[0]} records to insert into prod")

        try: 
            con = prod.get_sqlalchemy_engine()
            metadata = MetaData()
            table = Table('stock_news', metadata ,autoload_with=con)
            with con.begin() as conn:
                conn.execute(insert(table), results.to_dict(orient='records'))
                last_date = conn.execute('SELECT max("pubDate") FROM inv.public.stock_news').fetchone()[0]
            print(f"Last news publicated in prod is at {last_date}")     
            stg.run('TRUNCATE TABLE inv_stg.public.stg_stock_news RESTART IDENTITY CASCADE;')
        except Exception as e:
            print(e)




    tickers = fetch_list_of_tickers()
    fetch_news_task = fetch_news_from_api.expand(ticker=tickers)
    aggregated_news_task  = aggregate_news(fetch_news_task)
    insert_stg_task = insert_into_stg(aggregated_news_task)
    insert_prod_task = insert_into_prod()

    tickers >> fetch_news_task >> aggregated_news_task >> insert_stg_task >> insert_prod_task
fetch_news()
