import os
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ---------------------------------------------------
# 1. 建立資料庫連線（讀環境變數）
# ---------------------------------------------------
load_dotenv()

username = os.getenv("GCP_username")
password = os.getenv("GCP_password")
server   = os.getenv("host")
port     = os.getenv("port")

# 你有兩個資料庫：test_night_market、test_accident
DB_NIGHT_MARKET = "test_night_market"
DB_ACCIDENT     = "test_accident"

def get_engine(db_name):
    conn_str = f"mysql+pymysql://{username}:{password}@{server}:{port}/{db_name}?charset=utf8mb4"
    return create_engine(conn_str)

# ---------------------------------------------------
# 2. 夜市資料（Night_market_merge）
# ---------------------------------------------------
def load_night_markets(columns="*"):
    engine = get_engine(DB_NIGHT_MARKET)
    query = f"SELECT {columns} FROM Night_market_merge"
    return pd.read_sql(query, engine)

# ---------------------------------------------------
# 3. 事故主表（accident_new_sq1_main）
#    支援：欄位選擇 + 日期篩選
# ---------------------------------------------------
def load_accidents(columns="*", start_date=None, end_date=None):
    engine = get_engine(DB_ACCIDENT)
    query = f"SELECT {columns} FROM accident_new_sq1_main WHERE 1=1"

    if start_date and end_date:
        query += f" AND accident_datetime BETWEEN '{start_date}' AND '{end_date}'"

    df = pd.read_sql(query, engine)

    # ⭐ 只做 tooltip（滑過顯示）
    df["tooltip_text"] = df.apply(
        lambda row: (
            f"事故時間：{row['accident_datetime'].strftime('%Y-%m-%d %H:%M')}\n"
            f"死亡：{int(row['death_count'])} 人\n"
            f"受傷：{int(row['injury_count'])} 人"
        ),
        axis=1
    )

    return df


# ---------------------------------------------------
# 4. 環境因子（accident_new_sq1_env）
# ---------------------------------------------------
def load_env_factors(columns="*"):
    engine = get_engine(DB_ACCIDENT)
    query = f"SELECT {columns} FROM accident_new_sq1_env"
    return pd.read_sql(query, engine)

# ---------------------------------------------------
# 5. 人為因子（accident_new_sq1_process）
# ---------------------------------------------------
def load_human_factors(columns="*"):
    engine = get_engine(DB_ACCIDENT)
    query = f"SELECT {columns} FROM accident_new_sq1_process"
    return pd.read_sql(query, engine)
