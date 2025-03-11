Overview
========

This project is a training attempt at running airflow, creating dags and maintaining the environment.This will be built on top of Astronomers Airflow installation. This is to server for educational purposes.


**Requirements**

- This project has been built on top of Astronomer CLI. You can find information about
the required packages in both the Dockerfile and Requirements.txt

**Structure**

- DAGs in the project are located inside of the DAGs folder.
- DBT project is installed inside of Dags/ this could probably be refactored
for a better project structure.
  - All of the SQL models (Silver tier) are located inside models/finanças
  - You can find some Jinja macros inside of the macro folders. These are used to run some simple scripts
  - Metadata for the models are to be defined in the .yml file in their parent folders

**Reproducing**

This project has not been created with the goal of being reproduceable. 
It was simply put together as a repo so that I could practice git, workflows and so that
I have a backup in case things go **KABOOM**


