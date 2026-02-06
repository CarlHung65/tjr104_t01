from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from pathlib import Path

# Connect to GCP VM MySQL server
load_dotenv()
username = quote_plus(os.getenv("user"))
password = quote_plus(os.getenv("passwd"))
host = quote_plus(os.getenv("host"))
port = quote_plus(os.getenv("port"))
charset = quote_plus(os.getenv("charset"))
db_name = "test_weather"

# 如.env沒有mail_adress，以jessie為預設
writer = quote_plus(os.getenv("mail_address", "jessie"))

# {host}:{port}應該要是 localhost:3307? 、 127.0.0.1:3307? 、mysql:3307?
engine = create_engine(
    f"mysql+pymysql://{username}:{password}@{host}:{port}/{db_name}?charset={charset}",
    echo=False,  # 使用預設值，不印SQL日誌，保持乾淨輸出，生產環境適用
    pool_pre_ping=True,  # 檢查連線有效性
    pool_recycle=300,    # 每5分鐘自動重整連線，可再調整
    connect_args={'connect_timeout': 60})


def create_hist_weahter_data_table(engine, table_name: str) -> None:
    """Create the table 'historical_weather_data' into the assigned database
    in MySQL server (given by engine object) if the table is not exist.
    Raise Error when the table is already exist. '"""
    with engine.connect() as conn:
        ddl_query = text(f"""CREATE TABLE {table_name}(
                                `hash_value` CHAR(64) NOT NULL,
                                `station_record_id` INT AUTO_INCREMENT COMMENT '測站紀錄流水編號',
                                `station_id` VARCHAR(10) COMMENT '觀測站別',
                                `observation_datetime` DATETIME COMMENT '氣象觀測日期(yyyy/mm/dd HH:MM:00)',
                                `station_air_pressure` DECIMAL(6,1) COMMENT '測站氣壓(hPa)',
                                `sea_pressure` DECIMAL(6,1) COMMENT '海平面氣壓(hPa)',
                                `temperature` DECIMAL(6,1) COMMENT '氣溫(℃)',
                                `temp_dew_point` DECIMAL(6,1) COMMENT '露點溫度(℃)',
                                `relative_humidity` DECIMAL(6,1) COMMENT '相對溼度(%)',
                                `wind_speed` DECIMAL(6,1) COMMENT '風速(m/s)',
                                `wind_direction` DECIMAL(6,1) COMMENT '風向(360degree)',
                                `wind_speed_gust` DECIMAL(6,1) COMMENT '最大瞬間風(m/s)',
                                `wind_distant_gust` DECIMAL(6,1) COMMENT '最大瞬間風風向(360degree)',
                                `precipitation` DECIMAL(6,1) COMMENT '降水量(mm)',
                                `precipitation_hour` DECIMAL(6,1) COMMENT '降水時數(h)',
                                `sun_shine_hour` DECIMAL(6,1) COMMENT '日照時數(h)',
                                `global_radiation` DECIMAL(6,1) COMMENT '全天空日射量(MJ/㎡)',
                                `visibility` DECIMAL(6,1) COMMENT '能見度(km)',
                                `visibility_mean_auto` DECIMAL(6,1) COMMENT '能見度_自動(km)',
                                `UVI` DECIMAL(6,1) COMMENT '紫外線指數',
                                `cloud_amount` DECIMAL(6,1) COMMENT '總雲量(0~10)',
                                `cloud_amount_by_satellites` DECIMAL(6,1) COMMENT '總雲量_衛星(0~10)',
                                `soil_temp_at_0_cm` DECIMAL(6,1) COMMENT '地溫0cm',
                                `soil_temp_at_5_cm` DECIMAL(6,1) COMMENT '地溫5cm',
                                `soil_temp_at_10_cm` DECIMAL(6,1) COMMENT '地溫10cm',
                                `soil_temp_at_20_cm` DECIMAL(6,1) COMMENT '地溫20cm',
                                `soil_temp_at_30_cm` DECIMAL(6,1) COMMENT '地溫30cm',
                                `soil_temp_at_50_cm` DECIMAL(6,1) COMMENT '地溫50cm',
                                `soil_temp_at_100_cm` DECIMAL(6,1) COMMENT '地溫100cm',
                                `created_on` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立日期',
                                `created_by` VARCHAR(50) NOT NULL COMMENT '建立者',
                                `updated_on` TIMESTAMP COMMENT '最近修改日期',
                                `updated_by` VARCHAR(50) COMMENT '修改者',
                                PRIMARY KEY (`hash_value`),
                                CONSTRAINT FK_HWD_StnID FOREIGN KEY (`station_record_id`)
                                            REFERENCES obs_stations(`station_record_id`))
                                CHARSET=utf8mb4 COMMENT '各觀測站天氣觀測結果';""")

        conn.execute(ddl_query)
        print(f"{table_name}資料表建立成功！")
        return None


# 創建資料表
create_hist_weahter_data_table(engine, "weather_hourly_history")
