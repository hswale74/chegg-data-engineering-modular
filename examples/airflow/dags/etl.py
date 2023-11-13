from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import random

#define the name and schedule for the DAG
my_dag = DAG(
    dag_id="myFirstETL",
    start_date=datetime(year=2023, month=1, day=1),    
    schedule_interval='@daily',
    catchup=False  #only run data collection periods in the future. No need to "catch up"
)

#define some functions to define the logic of your tasks
def extract(**kwargs):
    ti = kwargs.get('ti')
    #create some fake data
    data = {
        'a': random.randint(1,10),  #a random number between 1 and 10
        'b': random.randint(-10,-1) #a random number between -10 and -1
        }
    ti.xcom_push("data", data) #push the dict data to xcom under the key 'data'

def transform(**kwargs):
    ti = kwargs.get('ti')
    #add up the values for all the keys in data
    data = ti.xcom_pull(task_ids='extract', key='data') #pull the key 'data' from task 'extract'
    ti.xcom_push('agg_data', sum(data.values())) #push the sum of data.values to xcom under key 'agg_data'

def load(file_path, **kwargs):
    ti = kwargs.get('ti')
    agg_data = ti.xcom_pull(task_ids='transform', key='agg_data') #pull the key 'agg_data' from the task 'tranform'
    #Append a new line to my data file with a timestamp plus the aggregated data
    with open(file_path, 'a') as file:
        file.write(f"{datetime.now()}: {agg_data}\n")


#turn functions into tasks
extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract,
    dag=my_dag
)

transform_task = PythonOperator(
    task_id="transform",
    python_callable=transform,
    dag=my_dag,
    trigger_rule='all_done'
)

load_task = PythonOperator(
    task_id="load",
    python_callable=load,
    dag=my_dag,
    op_kwargs={'file_path': "data/data.txt"},
    trigger_rule='all_done'
)

#define the order of operations
extract_task >> transform_task >> load_task

