import sys
import os
# 強制將專案根目錄 (/opt/airflow) 加入搜尋路徑
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow import DAG
#from airflow.operators.python import PythonOperator
#修正 Deprecated 警告
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta


# 1. 確保 Airflow 找到你的 src 邏輯
sys.path.append('/app')

# 2. 引入你剛才整合好的主程式進入點
# 假設你的主程式檔名是 main_etl.py
from src.job_accident.main_pipeline import run_accident_full_pipeline

# 3. 指揮官設定 (剛才討論的 retry 邏輯)
default_args = {
    'owner': 'andrew',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False, # 暫時不發信
}

# 4. 任務排程
with DAG(
    dag_id='accident_gcp_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # 這個任務會去執行你寫在 main_etl.py 裡的 pipeline
    # 包含：讀取 .env -> 爬蟲 -> 清洗 -> 檢查資料表 -> 上傳 GCP
    task_execute_etl = PythonOperator(
        task_id='run_full_process',
        python_callable=run_accident_full_pipeline
    )

    task_execute_etl