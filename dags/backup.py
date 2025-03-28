from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

"""
This DAG is to server as a backup option for both of the databases on the project. Because this project runs on docker 
and my database is in of them, I might lose the data on them in case I 'accidentally' do something to the astro version. 
It only backups up the actual data as of now.
"""

stg = BaseHook.get_connection("local_pg_stg")
prod = BaseHook.get_connection("local_pg")

@dag(dag_id = "backup_pg", schedule_interval = "@weekly", start_date = datetime(2023, 1, 1), catchup = False)

def backup_pg():
    backup_stg = BashOperator(
        task_id = "backup_stg",
        bash_command=f"""PGPASSWORD='{stg.password}' pg_dump -U {stg.login} -h {stg.host} {stg.schema} > /usr/local/airflow/dags/backup/backupstg.sql"""

    )

    backup_prod = BashOperator(
        task_id = "backup_prod",
        bash_command=f"""
            PGPASSWORD='{prod.password}' pg_dump -U {prod.login} -h {prod.host} {prod.schema} > /usr/local/airflow/dags/backup/backupprod.sql"""
    )
    backup_stg >> backup_prod

backup_pg()