# import des librairies
import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
# déclaration par annotation
@dag(
    dag_id="declaration_annotation",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily")
def generate_dag():
    # appel d'execution d'une tache dans mon DAG
    EmptyOperator(task_id="task")
# execution du DAG
generate_dag()