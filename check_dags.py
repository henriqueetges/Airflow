import os
from airflow.models import DagBag

DAG_FOLDER = os.path.join(os.getenv("GITHUB_WORKSPACE", ""), "dags")

def check_dags():
    dag_bag = DagBag(dag_folder=DAG_FOLDER)
    if dag_bag.import_errors:
        raise Exception(f'Import errors occurred, {dag_bag.import_errors}')
    print('All DAGs Imported')

if __name__ == '__main__':
    check_dags()