import sys
import os


# --- 強制路徑設定區 (必須放在所有 src import 之前) ---
# 獲取專案根目錄的絕對路徑 /opt/airflow
BASE_DIR = os.path.abspath("/opt/airflow")
JOB_DIR = os.path.join(BASE_DIR, "src/job_accident")
# 確保這些路徑出現在搜尋清單的最前面
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
if JOB_DIR not in sys.path:
    sys.path.insert(0, JOB_DIR)

from airflow import DAG
#修正 Deprecated 警告
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from main_pipeline import run_accident_full_pipeline

# 3. 指揮官設定 (剛才討論的 retry 邏輯)
default_args = {
    'owner': 'andrew',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False, # 暫時不發信
}

# 4. 任務排程
with DAG(
    dag_id='accident_gcp_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    # 這個任務會去執行你寫在 main_etl.py 裡的 pipeline
    # 包含：讀取 .env -> 爬蟲 -> 清洗 -> 檢查資料表 -> 上傳 GCP
    task_execute_etl = PythonOperator(
        task_id='run_full_process',
        python_callable=run_accident_full_pipeline
    )

    task_execute_etl