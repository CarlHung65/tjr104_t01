from pathlib import Path
import time
from datetime import timedelta
import pandas as pd
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import sys

# 1. 先確保路徑進去了
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# 2. 在 sys.path 之後才進行 import
from tasks.e_get_unique_traffic_geo_gcp import e_get_unique_traffic_geo
from tasks.e_crawler_weather_gcp_refactor import e_crawler_weather_refactor
from tasks.l_load_to_mysql_gcp import l_load_to_mysql


# 定義基礎路徑 (對應docker run 的掛載路徑)
BASE_PATH = "/opt/airflow/data"


# Default arguments for the DAG
default_args = {
    "owner": "airflow-tjr104",  # DAG 擁有者名稱
    "depends_on_past": False,  # 任務是否依賴前一次執行結果（False=獨立執行）
    "email": ["lucky460721@example.com"],  # 通知收件人
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,  # 失敗時重試2次
    "retry_delay": timedelta(minutes=10),  # 重試間隔10分鐘
}


@dag(
    dag_id='d_accident_weather_data_distributed_refactor',
    default_args=default_args,
    description="DAG for four Docker container to request weather API until load to MySQL",
    schedule=None,
    start_date=datetime(2026, 2, 10),
    catchup=False,
    tags=['traffic', 'weatherapi', 'taskflow'],
)
def accident_weather_pipeline():
    @task
    def l_summary_report(parquet_dir: str | Path, year: int) -> None:
        """
        L: 載入與確認 - 檢查最終產出檔案數量
        """
        files = list(Path(parquet_dir).glob("*.parquet"))
        print(f"============== Summary: {year} ==============")
        print(f"Year: {year}")
        print(f"Storage path: {parquet_dir}")
        print(f"Total Parquet files collected: {len(files)}")
        print("==============================================")
        return None

    # 定義 2021-2024、2026 的任務流

    # 定義年份分配表
    VM_YEAR_MAP = {1: [2021], 2: [2022],
                   3: [2023], 4: [2024, 2026],  # VM4 負擔剩餘年份
                   5: [], }
    # 從環境變數讀取 VM_ID
    vm_id = int(os.getenv("VM_ID", 1))

    if vm_id in [1, 2, 3, 4]:
        target_years = VM_YEAR_MAP.get(vm_id, [])
        for year in target_years:
            unique_geo = e_get_unique_traffic_geo(year)
            parquet_saved_in = e_crawler_weather_refactor(unique_geo, year)

    elif vm_id == 5:
        # VM5 負責彙整 2021-2024、2026 所有已爬取的資料進 MySQL
        for year in [2021, 2022, 2023, 2024, 2026]:
            # 去掉對e_.py腳本的依賴，因為是由另一個實體vm1-4完成。
            l_load_to_mysql(year, "test_weather", unique_geo)
            l_summary_report(parquet_saved_in, year)


# Instantiate the DAG
accident_weather_pipeline()
