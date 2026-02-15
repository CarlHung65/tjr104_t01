from pathlib import Path
from datetime import timedelta
from datetime import datetime
from airflow.sdk import dag, task
from airflow.utils.task_group import TaskGroup
import os
import sys

# 1. 先確保路徑進去了
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# 2. 在 sys.path 之後才進行 import
from tasks.e_crawler_weather_gcp_refactor import e_get_uniq_acc_geo, prep_batch_plan, e_crawler_weatherapi
from tasks.l_load_to_mysql_gcp import l_load_to_mysql


# 定義基礎路徑 (對應docker run 的掛載路徑)
BASE_PATH = "/opt/airflow/data"


# Default arguments for the DAG
default_args = {
    "owner": "airflow-tjr104",  # DAG 擁有者名稱
    "depends_on_past": False,  # 任務是否依賴前一次執行結果（False=獨立執行）
    "retries": 2,  # dag run失敗時最多重試2次，總計允許執行3次
    "retry_delay": timedelta(minutes=10),  # 除非task自己有額外定義，否則task重試需間隔60分鐘
}


@dag(
    dag_id='d_accident_weatherapi_dynamic_mapping',
    default_args=default_args,
    description="DAG for one Docker container to request weather API until load to MySQL",
    schedule=None,
    start_date=datetime(2026, 2, 13),
    catchup=False,
    tags=['traffic', 'weatherapi', 'taskflow'],
)
def accident_weather_pipeline():
    @task
    def l_summary_report(target_year: int, upstream) -> None:
        """
        L: 載入與確認 - 檢查最終產出檔案數量
        """
        parquet_dir = f"{BASE_PATH}/weather_cache/{target_year}"
        files = list(Path(parquet_dir).glob("*.parquet"))
        print(f"============== Summary: {target_year} ==============")
        print(f"Year: {target_year}")
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
    vm_id = int(os.getenv("VM_ID"))

    if vm_id in [1, 2, 3, 4]:
        target_years = VM_YEAR_MAP.get(vm_id, [])
        for year in target_years:
            df = e_get_uniq_acc_geo(year)
            batches = prep_batch_plan(df, year, batch_size=50)

            with TaskGroup(group_id=f"crawling_for_{year}"):
                # MappedOperator
                craw_done = e_crawler_weatherapi.partial(
                    target_year=year).expand(df_one_batch=batches)
                report_done = l_summary_report.partial(
                    target_year=year).expand(upstream=craw_done)
                l_load_to_mysql.partial(
                    target_year=year, database="test_weather", df_uniq_loc_acc=df).expand(upstream=report_done)


# Instantiate the DAG
accident_weather_pipeline()
