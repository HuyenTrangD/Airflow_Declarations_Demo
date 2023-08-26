# import des libraires
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from random import randint

# arguments par defaut
default_args = {
    "start_date": datetime(2023,7,12),
    "retries": 1
}

#ma fonction aux choix multple
def _mon_eval_model():
    accuracy = randint(0,100)
    print("accuracy : ", accuracy)
    if accuracy > 70:
        return ["super_accurate", "accurate"]
    elif accuracy > 50:
        return "accurate"
    return "not_accurate"

# declaration de mon dag par contexte
with DAG(
    dag_id="demo_branch_optionel",
    schedule="@once",
    catchup=False,
    default_args=default_args
) as dag:
    # tache 1 simule creation du model
    t1 = DummyOperator(
        task_id="create_model"
    )
    # tache de branchement sur son evaluation
    choose_best = BranchPythonOperator(
        task_id="choose_best",
        python_callable=_mon_eval_model
    )
    # tache du choix "super_accurate"
    super_accurate = DummyOperator(
        task_id="super_accurate"
    )
    # tache du choix "accurate"
    accurate = DummyOperator(
        task_id="accurate"
    )
    # tache du choix "not_accurate"
    not_accurate = DummyOperator(
        task_id="not_accurate"
    )
    
    # relation entres mes taches
    t1 >> choose_best >> [super_accurate, accurate, not_accurate]