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
from src.create_table.create_accident_table import ONE_PAGE_URL, GCP_DB_URL
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
        """任務 1: 爬取與清洗數據"""
        print(f"開始抓取來源: {ONE_PAGE_URL}")
        old_data = auto_scrape_and_download_old_data(ONE_PAGE_URL)
        trans_data = transform_data_dict(old_data)
        cleaned_data = car_crash_old_data_clean(trans_data)
        
        # 為了節省記憶體，我們回傳清理後的內容給下一個任務
        # 注意：大型 DataFrame 建議存成臨時 CSV 或 Parquet，這裡我們先採記憶體傳遞
        return {
            'main': cleaned_data['main'].to_json(),
            'party': cleaned_data['party'].to_json()
        }

    @task
    def load_to_mysql(cleaned_json: dict):
        """任務 2: 載入資料庫"""
        import pandas as pd
        import gc
        
        print("正在載入至 GCP MySQL TMP Table...")
        clean1 = pd.read_json(cleaned_json['main'])
        clean2 = pd.read_json(cleaned_json['party'])
        
        load_to_GCP_mysql_tmp_table(clean1, clean2)
        
        # 手動釋放資源
        del clean1, clean2
        gc.collect()
        return "Load Success"

    @task
    def setup_relations():
        """任務 3: 建立 PK/FK"""
        print("開始統一建立資料庫關聯 (PK/FK)...")
        engine = create_engine(GCP_DB_URL)
        setting_TMP_pkfk(engine)
        engine.dispose()
        print("✨ 暫存環境建立完成。")

    # --- 執行流程 ---
    data = extract_and_transform()
    load_status = load_to_mysql(data)
    load_status >> setup_relations()