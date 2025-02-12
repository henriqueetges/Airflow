from airflow.decorators import task, dag
import logging
from datetime import datetime
import subprocess

@dag(
    dag_id="run_dbt"
    , schedule="@weekly"
    , catchup=False
    , start_date=datetime(2025, 1, 1)
    , tags=['environment']
)
def run_dbt_dag():

    @task
    def dbt_run():
        dbt_dir = '/usr/local/airflow/dags/carteira_dbt_airflow'
        print('Running dbt')
        try:
            process = subprocess.Popen(
                ['dbt', 'run']
                , cwd=dbt_dir
                , stdout=subprocess.PIPE
                , stderr=subprocess.STDOUT
                , text=True)
            for line in iter(process.stdout.readline, ""):
                logging.info(line.strip())
            process.stdout.close()
            return_code = process.wait()

            if return_code != 0:
                raise RuntimeError(f"dbt run failed with return code {return_code}")
            
            logging.info("dbt run completed successfully!")

        except Exception as e:
            print(e)
    
    dbt_run()
run_dbt_dag()