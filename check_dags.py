import os
from airflow.models import DagBag

def check_dags():
    dag_bag = DagBag()
    if dag_bag.import_errors:
        raise Exception(f'Import errors occurred, {dag_bag.import_errors}')
    print('All DAGs Imported')

if __name__ == '__main__':
    check_dags()