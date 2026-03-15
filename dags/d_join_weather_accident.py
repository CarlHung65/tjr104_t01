from datetime import timedelta, datetime, timezone
from pathlib import Path
import sys
import os
import pymysql
from airflow.sdk import dag, task

# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到./tasks ./utils下的模組或套件
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# Default arguments for the DAG
default_args = {
    "owner": "tjr104-t01-04-jessie",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="d_join_weather_and_accident",
    default_args=default_args,
    description="Periodically update the mart table of weather observation data related "
                "to the locations of traffic accidents",
    schedule="00 12 */3 * *",  # 每月1、4、7、10、13、16日的12點00分執行一次
    start_date=datetime(2026, 3, 9, 12, 00,
                        tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=['traffic', 'weather', 'join', 'mart layer'],
)
def d_join_weather_and_accident():

    @task()
    def read_sql(sql_file_path: Path) -> list[str]:
        with sql_file_path.open(mode="r") as f:
            list_of_sql_lines = f.read().split(";")
        return list_of_sql_lines

    @task()
    def exec_sql_linebyline(list_of_sql_lines: list[str]) -> None:
        username = os.getenv("MYSQL_USER")
        password = os.getenv("MYSQL_PASSWORD")
        host = os.getenv("MYSQL_HOST", "mysql8")
        port = os.getenv("MYSQL_PORT", 3306)
        charset = os.getenv("CHARSET", "utf8mb4")
        try:
            conn = pymysql.connect(host=host,
                                   port=int(port),
                                   user=username,
                                   password=password,
                                   database="car_accident",
                                   charset=charset,
                                   autocommit=False,
                                   connect_timeout=60,
                                   read_timeout=600,
                                   write_timeout=600,
                                   )
            cursor = conn.cursor()
            for line in list_of_sql_lines:
                cursor.execute(str(line))
                conn.commit()
        except Exception as e:
            print(
                f"Error on executing the SQL statement: [{line[0:50]}]. Error msg: {e}!")
            conn.rollback()
        else:
            print("Mart層資料表建立成功!")
        finally:
            cursor.close()
            conn.close()
        return None

    sql_file_path = Path().resolve() / \
        "mart_table/Mart_weather_at_accident_locations_this_year.sql"
    sql_in_list = read_sql(sql_file_path)
    exec_sql_linebyline(sql_in_list)


# Instantiate the DAG
d_join_weather_and_accident()
