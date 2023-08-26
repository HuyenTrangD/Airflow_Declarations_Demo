# import des librairies
import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# déclaration par constructeur
my_dag = DAG(
dag_id="declaration_constructeur",
start_date=datetime.datetime(2021, 1, 1),
schedule="@daily",
)
# appel d'execution d'une tache dans mon DAG
EmptyOperator(task_id="task", dag=my_dag)