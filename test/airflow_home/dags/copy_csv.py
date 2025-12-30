from datetime import datetime
import os
import shutil

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

local_tz = pendulum.timezone("Asia/Kolkata")

INPUT_DIR = "/Users/kara/Desktop/airflow/test/input"
ARCHIVE_DIR = "/Users/kara/Desktop/airflow/test/output"
FILENAME = "access.csv"


def copy():
    src = os.path.join(INPUT_DIR, FILENAME)
    dst = os.path.join(ARCHIVE_DIR, FILENAME)

    if not os.path.exists(src):
        raise FileNotFoundError(f"{src} does not exist")

    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    shutil.copy2(src, dst)
    print(f"Copied {src} → {dst}")


with DAG(
    dag_id="copy",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule="37 3 * * *",   # 3:20 AM IST
    catchup=False,
    tags=["local", "csv"],
) as dag:

    copy = PythonOperator(
        task_id="copy",
        python_callable=copy,
    )
