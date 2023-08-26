import datetime
# import des librairies airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
with DAG(
  dag_id="declaration_contexte",
  start_date = datetime.datetime(2021, 1, 1),
  schedule="@daily",
  default_args={"key":"value"}
):
  # contexte d'execution de mes taches
  EmptyOperator(task_id="task")