from sqlalchemy import text
import sys
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from airflow.sdk import task
from airflow.models import Variable
from airflow.exceptions import AirflowException

# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到 ./utils下的模組或套件
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# 2. 在sys.path之後才進行import
from src.job_weather.OpenMeteo_crawler_weather.utils.create_engine_conn_tomysql import get_conn_pymysql
from src.job_weather.OpenMeteo_crawler_weather.utils.create_weather_related_tables import create_bridge_table


@task(retries=2, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(hours=4))
def l_load_to_bridge_table(target_year: int, *, database: str | None = None,
                           upstream) -> None:
    """"""
    # 1. 設定要橋接的資料表名稱
    this_year = datetime.now().year
    w_table_name = "weather_hourly_history" if target_year < this_year else "weather_hourly_now"
    a_table_name = "accident_sq1_main" if target_year < this_year else "accident_new_sq1_main"
    bridge_table_name = "accident_weather_bridge_history" if target_year < this_year else "accident_weather_bridge_now"
    create_bridge_table(w_table_name, a_table_name,
                        bridge_table_name, database=database)

    # 2. 準備與MySQL server的連線，用作為底層驅動的pymysql建立conn
    conn = get_conn_pymysql(database)
    try:
        # 3. 用作為底層驅動的pymysql建立conn＆cursor
        cursor = conn.cursor()
        dml_str = f"""INSERT INTO {bridge_table_name} 
                            (accident_id, weather_record_id, 
                                observation_datetime, longitude_round, latitude_round)
                            SELECT a_tmp.accident_id, w.weather_record_id, 
                                w.observation_datetime, w.longitude_round, w.latitude_round
                                FROM (
                                        SELECT accident_id,
                                            TIMESTAMP(date(accident_datetime),
                                                        SEC_TO_TIME(ROUND(
                                                                        TIME_TO_SEC(
                                                                            time(accident_datetime)) / 3600
                                                                        ) * 3600
                                                                    )
                                                    ) as approx_accident_datetime, 
                                            round(longitude, 2) as `longitude_round`,
                                            round(latitude, 2) as `latitude_round`    
                                        FROM {a_table_name}
                                        WHERE YEAR(accident_datetime) = {target_year}
                                    ) a_tmp
                                INNER JOIN {w_table_name} w ON 
                                    w.observation_datetime = a_tmp.approx_accident_datetime AND 
                                    w.longitude_round = a_tmp.longitude_round AND
                                    w.latitude_round = a_tmp.latitude_round
                                ON DUPLICATE KEY UPDATE 
                                observation_datetime = VALUES(observation_datetime)
                            ;"""
        cursor.execute(dml_str)
        conn.commit()
    except Exception as e:
        print(f"Error on inserting into bridging table, Error msg: {e}")
        conn.rollback()
    else:
        print(f"Successfully inserting into bridging table")
    finally:
        cursor.close()
        conn.close()
    return None
