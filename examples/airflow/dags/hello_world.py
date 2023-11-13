from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


myHelloWorldDag = DAG(
    dag_id='hello_world',
    schedule_interval="@daily",
    start_date=datetime(year=2023,month=1,day=1),
    catchup=False
)

def print_hello_function():
    print("Hello World!")

def print_goodbye_function():
    print("Goodbye World!")

print_hello_task = PythonOperator(
    task_id="print_hello_task",
    python_callable=print_hello_function,
    dag=myHelloWorldDag
)

print_goodbye_task = PythonOperator(
    task_id="print_goodbye_task",
    python_callable=print_goodbye_function,
    dag=myHelloWorldDag
)

print_hello_task >> print_goodbye_task


