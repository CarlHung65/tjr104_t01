from t_dataclr_acc_nearby_obs_stn import nearby_Obs_stn
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

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


try:
    with engine.begin() as conn:
        # 存入SQL server。append
        nearby_Obs_stn["created_by"] = writer
        nearby_Obs_stn.to_sql("accident_nearby_obs_stn", con=conn, index=False,
                              if_exists="append", method="multi", chunksize=200)
except Exception as e:
    print(f"Error: {e}")
else:
    print("資料表accident_nearby_obs_stn成功插入！")
