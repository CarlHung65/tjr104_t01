from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from pathlib import Path


def create_accident_nearby_stn_table(engine, table_name: str) -> None:
    """Use to create TABLE onto a MySQL database. This function 
    include the create a table and add neccessary primary key"""
    try:
        with engine.connect() as conn:
            ddl_text = text(f"""CREATE TABLE {table_name}(
                                `accident_id` VARCHAR(100),
                                `accident_datetime` DATETIME COMMENT '車禍發生日期時間',
                                `station_record_id` INT AUTO_INCREMENT COMMENT '測站紀錄流水編號',
                                `station_id` VARCHAR(10) COMMENT '附近觀測站別',
                                `state_valid_from` DATE COMMENT '運作狀態起始日',
                                `distance` DECIMAL(10, 6) COMMENT '距離',
                                `rank_of_distance` INT COMMENT '距離排名',
                                `created_on` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立日期',
                                `created_by` VARCHAR(50) NOT NULL COMMENT '建立者',
                                `updated_on` TIMESTAMP COMMENT '最近修改日期',
                                `updated_by` VARCHAR(50) COMMENT '修改者',
                                PRIMARY KEY (`accident_id`,`rank_of_distance`),
                                CONSTRAINT FK_ANS_StnRId FOREIGN KEY (`station_record_id`)
                                            REFERENCES obs_stations(`station_record_id`))
                                CHARSET=utf8mb4 
                                COMMENT '各觀測站天氣觀測結果';""")

            conn.execute(ddl_text)
    except RuntimeError as re:
        print(f"錯誤!{re}")
    except Exception as e:
        print(f"發生非預期的錯誤：{e}")
    else:
        print(f"{table_name}資料表建立成功！")
    return None


if __name__ == "__main__":
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

    # 創建資料表
    create_accident_nearby_stn_table(engine, "accident_nearby_obs_stn")
