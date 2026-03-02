import os
import sys
sys.path.insert(0, '/opt/airflow')
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta


from src.job_accident.main_pipeline import run_accident_full_pipeline

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
    schedule=None,        # 建議先設為 None，手動測試成功後再改 @daily
    catchup=False,
    tags=['accident', 'gcp'],
) as dag:

    task_execute_etl = PythonOperator(
        task_id='run_full_process',
        python_callable=run_accident_full_pipeline,
        # 關鍵設定：因為你的 13 個檔案處理很久，必須取消超時限制
        execution_timeout=None, 
    )

    task_execute_etl