import os
import sys
# 確保路徑正確
sys.path.insert(0, '/opt/airflow')
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta,timezone
from sqlalchemy import create_engine

# 匯入你原本定義好的功能函式
from src.job_accident.main_pipeline import is_db_ready
from src.job_accident.e_crawler_accident import (
    auto_scrape_and_download_old_data, 
    auto_scrape_recent_data, 
    read_old_data_to_dataframe
)
from src.job_accident.t_dataclr_accident import (
    car_crash_old_data_clean, 
    transform_data_dict
)
from src.job_accident.l_tomysqlgcp_accident import (
    load_to_GCP_mysql, 
    load_cmp_to_new_GCP_mysql
)
from src.job_accident.l_setpkfk_accident import (
    setting_pkfk, 
    setting_new_pkfk
)
from src.create_table.create_accident_table import GCP_DB_URL, SAVE_OLD_DATA_DIR, SEQ_PAGE_URL

default_args = {
    'owner': 'andrew',
    'start_date': datetime(2026, 2, 25, 5, 00,
                        tzinfo=timezone(offset=timedelta(hours=8))),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='accident_historical_init',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['initialization']
) as dag:

    @task
    def check_initialization():
        """檢查資料庫是否已初始化"""
        engine = create_engine(GCP_DB_URL)
        ready = is_db_ready(engine)
        return ready

    @task
    def process_historical_files(db_is_ready: bool):
        """處理歷年資料 (原本的 for 迴圈)"""
        if db_is_ready:
            print("資料庫已初始化，跳過歷年資料匯入。")
            return "skipped"
        
        engine = create_engine(GCP_DB_URL)
        files = os.listdir(SAVE_OLD_DATA_DIR) if os.path.exists(SAVE_OLD_DATA_DIR) else []
        
        if len(files) > 0:
            for item in files:
                print(f"正在處理本地檔案: {item}")
                df_list = read_old_data_to_dataframe(os.path.join(SAVE_OLD_DATA_DIR, item))
                cleaned = car_crash_old_data_clean(transform_data_dict(df_list))
                load_to_GCP_mysql(cleaned['main'], cleaned['party'])
        else:
            for url in SEQ_PAGE_URL:
                print(f"正在抓取網路資料: {url}")
                df_list = auto_scrape_and_download_old_data(url)
                cleaned = car_crash_old_data_clean(transform_data_dict(df_list))
                load_to_GCP_mysql(cleaned['main'], cleaned['party'])
        
        setting_pkfk(engine)
        return "completed"


    # --- 定義任務流程 ---
    ready_status = check_initialization()
    hist_process = process_historical_files(ready_status)
    
   