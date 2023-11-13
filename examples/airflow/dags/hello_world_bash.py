from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


myHelloWorldDagBash = DAG(
    dag_id='hello_world_bash',
    schedule_interval="@daily",
    start_date=datetime(year=2023,month=1,day=1),
    catchup=False
)

hello_world_task = BashOperator(
    task_id="hello_world_bash_task",
    bash_command="echo 'Hello World'",
    dag=myHelloWorldDagBash
)

goodbye_world_task = BashOperator(
    task_id="goodbye_world_bash_task",
    bash_command="echo 'Goodbye World'",
    dag=myHelloWorldDagBash,
    trigger_rule="all_done" #specify the trigger rule
)

hello_world_task >> goodbye_world_task