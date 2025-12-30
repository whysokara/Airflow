from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello Python Operator")


with DAG(
    dag_id="python_operator",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
) as dag:



    task = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )


