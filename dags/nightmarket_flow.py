from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
sys.path.insert(0, '/opt/airflow')

# 動態取得路徑，確保 Airflow 能找到 src 資料夾
# 假設專案結構：
# /project_root/dags/
# /project_root/src/job_nightmarket/
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_FOLDER)
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

# 匯入你的自定義模組
# 注意：這需要 e_crawler_wiki_market.py 裡有 main() 函式
try:
    from job_nightmarket import e_crawler_wiki_market
    from job_nightmarket import e_crawler_market_inform
    import t_clean_schema
    import l_SQL_to_GCP
except ImportError as e:
    print(f"匯入模組失敗：{e}")

# 預設參數
default_args = {
    'owner': 'tjr104',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 定義 DAG
with DAG(
    'taiwan_nightmarket_etl_pipeline',
    default_args=default_args,
    description='ETL for nightmarket location and opening hour to SQL',
    schedule_interval='@monthly',  # 每月跑一次 或寫成 '0 0 1 * *'
    catchup=False,
    tags=['nightmarket'],
) as dag:

    # Task 1: 爬取維基百科清單 (Extract 1)
    task_wiki_spider = PythonOperator(
        task_id='fetch_wiki_market_list',
        python_callable=e_crawler_wiki_market.main, 
    )

    # Task 2: 呼叫 Google API 取得詳細資料 (Extract 2)
    task_google_api = PythonOperator(
        task_id='fetch_google_place_details',
        python_callable=e_crawler_market_inform.main,
    )

    # Task 3: 資料清洗 (Transform)
    task_clean_data = PythonOperator(
        task_id='clean_and_normalize_data',
        python_callable=t_clean_schema.main,
    )

    # Task 4: 寫入資料庫 (Load)
    task_load_to_sql = PythonOperator(
        task_id='load_to_gcp_mysql',
        python_callable=l_SQL_to_GCP.main,
    )

    # 設定依賴關係 (Workflow)
    task_wiki_spider >> task_google_api >> task_clean_data >> task_load_to_sql