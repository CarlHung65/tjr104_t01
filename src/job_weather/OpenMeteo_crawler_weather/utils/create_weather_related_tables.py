from sqlalchemy import text
import pandas as pd

import sys


# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到 ./utils下的模組或套件
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# 2. 在sys.path之後才進行import
from src.job_weather.OpenMeteo_crawler_weather.utils.create_engine_conn_tomysql import get_engine_sqlalchemy, get_conn_pymysql


def create_weather_hist_table(table_name: str, *, database: str | None = None) -> pd.DataFrame:
    """Create the table 'weather_data' into the assigned database
        in MySQL server (given by engine object) if the table is not exist.
    """

    # 準備與MySQL server的連線
    if database:
        engine = get_engine_sqlalchemy(database)
    else:
        engine = get_engine_sqlalchemy()

    with engine.connect() as conn:
        ddl_text = text(f"""CREATE TABLE IF NOT EXISTS {table_name}(
                                `id` BIGINT AUTO_INCREMENT,
                                `observation_datetime` DATETIME NOT NULL COMMENT '觀測日期時間(yyyy/mm/dd_HH:MM)',
                                `temperature_degree` DECIMAL(6,2) COMMENT '氣溫(℃)',
                                `apparent_temperature_degree` DECIMAL(6,2) COMMENT '體感溫度(℃)',
                                `rain_within_hour_mm` DECIMAL(6,2) COMMENT '前一小時內降雨量(mm)',
                                `precipitation_mm` DECIMAL(6,2) COMMENT '前一小時內降雨降雪量(mm)',
                                `visibility_m` DECIMAL(6,2) COMMENT '能見度(m)',
                                `weather_code` INT COMMENT '天氣代碼WMO code',
                                `wind_speed_10m_km_per_h` DECIMAL(6,2) COMMENT '風速(km_per_hour)'
                                `wind_gusts_10m_km_per_h` DECIMAL(6,2) COMMENT '最大陣風(km_per_hour)'
                                `longitude_round` DECIMAL(6,2) COMMENT '經度(小數點後二位)',
                                `latitude_round` DECIMAL(6,2) COMMENT '緯度(小數點後二位)',
                                `created_on` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立日期',
                                `created_by` VARCHAR(50) NOT NULL COMMENT '建立者',
                                `hash_value` CHAR(32) NOT NULL,
                                PRIMARY KEY (`id`),
                                UNIQUE KEY UK_WHH_hash (`hash_value`))
                                CHARSET=utf8mb4 COMMENT '各地天氣觀測結果';""")

        conn.execute(ddl_text)
        print(f"確立{table_name}資料表已存在或已新建成功！")
        return None


def create_view_table(ddl_str: str, *, database: str | None = None) -> None:
    """create any view table as ddl descriptions."""

    # 準備與MySQL server的連線
    if database:
        engine = get_engine_sqlalchemy(database)
    else:
        engine = get_engine_sqlalchemy()

    with engine.connect() as conn:
        conn.execute(text(str(ddl_str)))
        print("建立view表成功!")
    return None
