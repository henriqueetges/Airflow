import os
from airflow.models import DagBag

def validate_dags():
    dags_folder = os.getenv("GITHUB_WORKSPACE", ".") + "/dags"
    dag_bag = DagBag(dags_folder)

    if dag_bag.import_errors:
        for dag_id, error in dag_bag.import_errors.items():
            print(f"Error in DAG '{dag_id}': {error}")

        raise Exception(f"DAG import errors found: {len(dag_bag.import_errors)} errors")

    print("✅ All DAGs imported successfully!")

if __name__ == "__main__":
    validate_dags()
