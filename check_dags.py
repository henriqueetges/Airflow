import os
from airflow.models import DagBag

DAG_FOLDER = os.path.join(os.getenv("GITHUB_WORKSPACE", ""), "dags")

def check_dags():
    dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
    ignored_errors = ("airflow.exceptions.AirflowNotFoundException", "sqlalchemy.exc.OperationalError")

    import_errors = {
        dag_id: error for dag_id, error in dag_bag.import_errors.items()
        if not any(err_type in str(error) for err_type in ignored_errors)
    }
    if import_errors:
        raise Exception(f'Import errors occurred, {dag_bag.import_errors}')
    print('All DAGs Imported')

if __name__ == '__main__':
    check_dags()