from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# 確保路徑正確指向你的 src 
SRC_PATH = "/opt/airflow/src/job_nightmarket"
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

# 匯入你的 ETL 模組
import e_crawler_wiki_market
import e_crawler_market_inform
import t_clean_schema
import l_SQL_to_GCP

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'taiwan_nightmarket_etl',
    default_args=default_args,
    description='全台夜市資料自動化 ETL 流程',
    # 修正處：將 schedule_interval 改為 schedule
    schedule='@weekly', 
    catchup=False,
    tags=['nightmarket', 'ETL'],
) as dag:

    task_fetch_wiki = PythonOperator(
        task_id='fetch_wiki_list',
        python_callable=e_crawler_wiki_market.main,
    )

    task_fetch_google_details = PythonOperator(
        task_id='fetch_google_details',
        python_callable=e_crawler_market_inform.main,
    )

    task_clean_data = PythonOperator(
        task_id='clean_and_transform',
        python_callable=t_clean_schema.main,
    )

    task_load_to_sql = PythonOperator(
        task_id='load_to_mysql',
        python_callable=l_SQL_to_GCP.main,
    )

    # 設定依賴關係
    task_fetch_wiki >> task_fetch_google_details >> task_clean_data >> task_load_to_sql