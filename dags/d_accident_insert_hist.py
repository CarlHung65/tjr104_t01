import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.decorators import task

# 確保 Airflow 能找到 src 目錄
sys.path.insert(0, '/opt/airflow')

# 匯入你的功能模組
from src.job_accident.e_crawler_accident import auto_scrape_and_download_old_data
from src.job_accident.t_dataclr_accident import car_crash_old_data_clean, transform_data_dict
from src.job_accident.l_tomysqlgcp_accident import load_to_GCP_mysql_tmp_table
from src.job_accident.l_setpkfk_accident import setting_TMP_pkfk
from src.create_table.create_accident_table import ONE_PAGE_URL, GCP_DB_URL,SAVE_OLD_DATA_DIR
from sqlalchemy import create_engine

default_args = {
    'owner': 'andrew',
    'start_date': datetime(2026, 3, 1),
    'retries': 0, # 手動任務通常不建議自動重試，方便除錯
}

with DAG(
    dag_id='accident_manual_import_tmp',
    default_args=default_args,
    schedule=None,  # 手動觸發
    catchup=False,
    tags=['accident', 'manual', 'etl'],
    doc_md="""
    ### 手動數據匯入任務
    1. 爬取指定年度/頁面的交通意外原始資料。
    2. 清洗並轉換資料格式。
    3. 載入至 GCP MySQL 的 **TMP 暫存表**。
    4. 建立暫存表的 PK/FK 關聯。
    """
) as dag:

    @task
    def extract_and_transform():
        engine = create_engine(GCP_DB_URL)
        files = os.listdir(SAVE_OLD_DATA_DIR) if os.path.exists(SAVE_OLD_DATA_DIR) else []
        """任務 1: 爬取與清洗數據"""
        print(f"開始抓取來源: {ONE_PAGE_URL}")
        df_list = auto_scrape_and_download_old_data(ONE_PAGE_URL)
        cleaned = car_crash_old_data_clean(transform_data_dict(df_list))
        load_to_GCP_mysql_tmp_table(cleaned['main'], cleaned['party'])
        setting_TMP_pkfk(engine)

    extract_and_transform()