import sys
import os

# --- 核心路徑修正 ---
# 既然 src 已經被掛載到 dags 裡面，我們直接把這兩個路徑塞到最前面
sys.path.insert(0, '/opt/airflow/dags/src')
sys.path.insert(0, '/opt/airflow/dags/src/job_accident')

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# --- 匯入邏輯 (重要：不要寫 from src.job_accident...) ---
# 既然已經把路徑塞進 sys.path，直接匯入檔名即可避免層級衝突
from main_pipeline import run_accident_full_pipeline

default_args = {
    'owner': 'andrew',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='accident_gcp_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    task_execute_etl = PythonOperator(
        task_id='run_full_process',
        python_callable=run_accident_full_pipeline
    )

    task_execute_etl