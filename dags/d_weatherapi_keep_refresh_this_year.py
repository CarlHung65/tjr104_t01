from datetime import timedelta, datetime, timezone
from airflow.sdk import dag, task, TaskGroup
import sys

# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到./tasks ./utils下的模組或套件
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# 2. 在sys.path之後才進行import
from src.job_weather.OpenMeteo_crawler_weather.tasks.e_crawler_weather_gcp_refactor import e_get_uniq_acc_geo, prep_batch_plan, e_crawler_weatherapi
from src.job_weather.OpenMeteo_crawler_weather.tasks.l_load_to_mysql_gcp import l_transform_and_load_to_mysql, l_summary_report

# Default arguments for the DAG
default_args = {
    "owner": "tjr104-t01-04-jessie",  # DAG 擁有者名稱
    "depends_on_past": False,  # 任務是否依賴前一次執行結果（False=獨立執行）
    "retries": 2,  # dag run失敗時最多重試2次，總計允許執行3次
    "retry_delay": timedelta(minutes=10),  # 除非task自己有額外定義，否則task重試需間隔10分鐘
}


@dag(
    dag_id='d_weatherapi_keep_refresh_this_year',
    default_args=default_args,
    description="ETL process from requesting weather API for the weather data between 'January 01st~3-day-prior-to-today' until loading to MySQL database",
    schedule='00 05 */3 * *',  # 每3天的05點00分執行一次
    start_date=datetime(2026, 2, 25, 5, 00,
                        tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=['traffic', 'weatherapi', 'taskflow'],
)
def accident_weather_pipeline():
    this_year = 2026

    with TaskGroup(group_id=f"year_{this_year}") as year_group:
        df = e_get_uniq_acc_geo(this_year,
                                database="tjr104_t01")
        batches = prep_batch_plan(df, this_year, batch_size=50)
        # MappedOperator
        craw_done = e_crawler_weatherapi.partial(
            target_year=this_year).expand(df_one_batch=batches)

        report_done = l_summary_report(target_year=this_year,
                                       upstream=craw_done)
        load_done = l_transform_and_load_to_mysql(target_year=this_year,
                                                  database="tjr104_t01",
                                                  upstream=report_done)


# Instantiate the DAG
accident_weather_pipeline()
