from sqlalchemy import text
import pandas as pd

import sys


# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到 ./utils下的模組或套件
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# 2. 在sys.path之後才進行import
from utils.create_engine_conn_tomysql import get_engine_sqlalchemy, get_conn_pymysql


def create_weather_hist_table(database: str, table_name: str) -> pd.DataFrame:
    """Create the table 'weather_data' into the assigned database
        in MySQL server (given by engine object) if the table is not exist.
    """

    # 準備與GCP VM上的MySQL server的連線
    engine = get_engine_sqlalchemy(database)

    with engine.connect() as conn:
        ddl_text = text(f"""CREATE TABLE IF NOT EXISTS {table_name}(
                                `id` BIGINT AUTO_INCREMENT,
                                `hash_value` CHAR(32) NOT NULL,
                                `observation_datetime` DATETIME NOT NULL COMMENT '觀測日期時間(yyyy/mm/dd HH:MM)',
                                `temperature_degree` DECIMAL(6,2) COMMENT '氣溫(℃)',
                                `apparent_temperature_degree` DECIMAL(6,2) COMMENT '體感溫度(℃)',
                                `rain_within_hour_mm` DECIMAL(6,2) COMMENT '一小時內降雨量(mm)',
                                `precipitation_mm` DECIMAL(6,2) COMMENT '一小時內降雨降雪量(mm)',
                                `visibility_m` DECIMAL(6,2) COMMENT '能見度(m)',
                                `weather_code` INT COMMENT '天氣代碼WMO code',
                                `longitude_round` DECIMAL(6,2) COMMENT '經度(小數點後二位)',
                                `latitude_round` DECIMAL(6,2) COMMENT '緯度(小數點後二位)',
                                `created_on` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立日期',
                                `created_by` VARCHAR(50) NOT NULL COMMENT '建立者',
                                PRIMARY KEY (`id`),
                                UNIQUE KEY UK_WHH_hash (`hash_value`))
                                CHARSET=utf8mb4 COMMENT '各地天氣觀測結果';""")

        conn.execute(ddl_text)
        print(f"確立{table_name}資料表已存在或已新建成功！")
        return None
