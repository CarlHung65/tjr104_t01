from t_dataclr_observation_stations import df_new_stn, df_existing_stn_change
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime

# Step 1: 準備與GCP VM上的MySQL server的連線
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


def insert_to_mysql(engine, df_new_stn: pd.DataFrame) -> None:
    if not df_new_stn.empty:
        row_info_to_insert = []
        df_new_stn_l = df_new_stn.copy()
        # 遍歷每個資料列，把要插入MySQL的欄位名稱的值，做成list。
        for _, row in df_new_stn_l.iterrows():
            row_info_to_insert.append({
                "station_id": row["station_id"],
                "station_name": row["station_name"],
                "station_sea_level": row["station_sea_level"],
                "station_longitude_WGS84": row["station_longitude_WGS84"],
                "station_latitude_WGS84": row["station_latitude_WGS84"],
                "station_working_state": row["station_working_state_new"],
                "state_valid_from": row["state_valid_from"],
                "state_valid_to": datetime(9999, 12, 31).strftime("%Y-%m-%d"),
                "remark": row["remark"],
                "created_by": writer
            })

        # 迴圈結束後批次執行append
        try:
            with engine.begin() as conn:
                df_to_insert = pd.DataFrame(row_info_to_insert)
                # insert = append至Obs_stations
                df_to_insert.to_sql("obs_stations", conn, method="multi",
                                    index=False, if_exists="append")
        except Exception as e:
            print(f"Insert錯誤: {e}")
        else:
            print("Insert成功！")
    else:
        print("沒有需要在資料表obs_stations新增的資料。")
    return None


def update_on_mysql(engine, df_existing_stn_change: pd.DataFrame) -> None:
    if not df_existing_stn_change.empty:
        df_existing_stn_change_l = df_existing_stn_change.copy()
        for _, row in df_existing_stn_change_l.iterrows():
            if (row["station_working_state_new"] is np.nan) or (row["station_working_state_new"] is pd.NA):
                try:
                    with engine.begin() as conn:
                        dml_text = text("""UPDATE obs_stations
                                                SET
                                                    station_working_state = "Previous Run",
                                                    state_valid_to = (:closing_date),
                                                    updated_on = CURRENT_TIMESTAMP,
                                                    updated_by = (:writer)
                                                WHERE station_record_id = (:record_id);
                                        """)
                        conn.execute(dml_text, {"closing_date": datetime.now().strftime("%Y-%m-%d"),
                                                "writer": writer,
                                                "record_id": row["station_record_id"],
                                                })
                except Exception as e:
                    print(f"Update錯誤: {e}")
                else:
                    print("Update成功！")
    else:
        print("沒有需要在資料表obs_stations修改既定資料。")
    return None


# Step 2: 建立連線後寫入資料表test_weather.obs_stations
# Step 2-1:在資料表中，插入新資料列df_new_stn
insert_to_mysql(engine, df_new_stn)
# Step 2-2: 在資料表中，根據df_existing_stn_change修改既定資料列
update_on_mysql(engine, df_existing_stn_change)
