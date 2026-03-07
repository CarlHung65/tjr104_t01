import os
import pandas as pd
<<<<<<< HEAD
import pymysql
=======
>>>>>>> Tom
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

<<<<<<< HEAD
# 你有兩個資料庫：test_night_market、test_accident
DB_NIGHT_MARKET = "test_night_market"
DB_ACCIDENT     = "test_accident"
=======
DB_NIGHT_MARKET = "test_night_market"
DB_ACCIDENT     = "car_accident"
>>>>>>> Tom

def get_engine(db_name):
    conn_str = f"mysql+pymysql://{username}:{password}@{server}:{port}/{db_name}?charset=utf8mb4"
    return create_engine(conn_str)

# ---------------------------------------------------
<<<<<<< HEAD
# 2. 夜市資料（Night_market_merge）
=======
# 2. 夜市資料
>>>>>>> Tom
# ---------------------------------------------------
def load_night_markets(columns="*"):
    engine = get_engine(DB_NIGHT_MARKET)
    query = f"SELECT {columns} FROM Night_market_merge"
    return pd.read_sql(query, engine)

# ---------------------------------------------------
<<<<<<< HEAD
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
=======
# 3. 事故主表（支援欄位、日期、limit）
# ---------------------------------------------------
def load_accidents(
    columns="*",
    start_date=None,
    end_date=None,
    date_filter_type=None,   # ⭐ 新增
    limit=None,
    order_by=None
):
    engine = get_engine(DB_ACCIDENT)

    query = f"SELECT {columns} FROM accident_all_main WHERE 1=1"

    # 日期區間
    if start_date and end_date:
        query += f" AND accident_datetime >= '{start_date} 00:00:00'"
        query += f" AND accident_datetime <= '{end_date} 23:59:59'"

    # ⭐ 智慧 LIMIT（依照日期模式自動決定）
    if limit is None:
        if date_filter_type == "單一日期":
            limit = 3000
        elif date_filter_type == "日期區間（最新7天）":
            limit = 5000
        elif date_filter_type in ["月份", "年份（選月份）"]:
            limit = 10000
        else:
            limit = 5000

    query += f" LIMIT {limit}"

    return pd.read_sql(query, engine)

# ---------------------------------------------------
# 4. 環境因子
# ---------------------------------------------------
def load_env_factors(columns="*", accident_ids=None, limit=50000):
    engine = get_engine(DB_ACCIDENT)
    query = f"SELECT {columns} FROM accident_all_env"

    # ⭐ 如果有 accident_ids，就只撈主表有的資料
    if accident_ids:
        ids = ",".join(map(str, accident_ids))
        query += f" WHERE accident_id IN ({ids})"

    # ⭐ 如果沒有 accident_ids（例如其他頁面），就用 LIMIT
    else:
        query += f" LIMIT {limit}"

    return pd.read_sql(query, engine)

# ---------------------------------------------------
# 5. 人為因子
# ---------------------------------------------------
def load_human_factors(columns="*", accident_ids=None, limit=50000):
    engine = get_engine(DB_ACCIDENT)
    query = f"SELECT {columns} FROM accident_all_process"

    # ⭐ 如果有 accident_ids，就只撈主表有的資料
    if accident_ids:
        ids = ",".join(map(str, accident_ids))
        query += f" WHERE accident_id IN ({ids})"

    # ⭐ 如果沒有 accident_ids，就用 LIMIT
    else:
        query += f" LIMIT {limit}"

    return pd.read_sql(query, engine)

# CREATE OR REPLACE VIEW accident_all_main AS
# SELECT * FROM accident_sq1_main
# UNION ALL
# SELECT * FROM accident_new_sq1_main;

# CREATE OR REPLACE VIEW accident_all_env AS
# SELECT * FROM accident_sq1_env
# UNION ALL
# SELECT * FROM accident_new_sq1_env;

# CREATE OR REPLACE VIEW accident_all_process AS
# SELECT * FROM accident_sq1_process
# UNION ALL
# SELECT * FROM accident_new_sq1_process;

>>>>>>> Tom
