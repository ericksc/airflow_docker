from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def helloWorld():
    print(f"{pd.show_versions()}")

with DAG(dag_id="get_pandas_dag",
         start_date=datetime(2024,10,16, 14, 30, 0),
         schedule_interval="*/5 * * * *",
         catchup=False) as dag:
    task1 = PythonOperator(
        task_id="hello_world",
        python_callable=helloWorld)

task1