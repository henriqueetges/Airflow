from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime


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