from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def helloWorld():
    print("Hello World")

with DAG(dag_id="hello_world_dag_v2",
         start_date=datetime(2024,10,16, 8, 0, 0),
         schedule_interval="*/5 * * * *",
         catchup=False) as dag:
    task1 = PythonOperator(
        task_id="hello_world",
        python_callable=helloWorld)

task1