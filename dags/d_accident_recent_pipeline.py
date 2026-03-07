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
    'start_date': datetime(2026, 3, 6, 5, 00,
                        tzinfo=timezone(offset=timedelta(hours=8))),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='accident_recent_update',
    default_args=default_args,
    schedule='0 5 */3 * *',
    catchup=False,
    tags=['recent_data', 'auto_scrape']
) as dag:
    
    @task
    def process_recent_data_task():
        """抓取並更新近期資料"""
        print("開始抓取近期資料...")
        new_data = auto_scrape_recent_data()
        cleaned = car_crash_old_data_clean(transform_data_dict(new_data))
        db_engine = load_cmp_to_new_GCP_mysql(cleaned['main'], cleaned['party'])
        if db_engine:
            setting_new_pkfk(db_engine)
        return "done"
    process_recent_data_task()