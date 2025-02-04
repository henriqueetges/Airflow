from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# Define the DAG
dag = DAG(
    dag_id="test_postgres_connection",
    start_date=days_ago(1),
    schedule=None,  # Run manually
    catchup=False,
)

# Define a simple test query
test_query = PostgresOperator(
    task_id="test_postgres_connection",
    postgres_conn_id="local_pg",  # Change this if using a different connection
    sql="SELECT 1;",
    dag=dag,
)
