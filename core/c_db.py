import os
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 解決欄位顯示不完整問題: 確保能清楚看到所有欄位
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
load_dotenv()

# 使用 SQLAlchemy 來管理 MySQL 的連線池 (Pool)，應付高併發的查詢
# pool_recycle=300: 設定連線 300 秒後自動回收，避免 MySQL 預設的 wait_timeout 切斷閒置連線導致報錯
def get_db_engine(db_name=None):
    db_user = os.getenv("DB_USER", "root")
    db_pass = quote_plus(os.getenv("DB_PASS"))
    
    if os.getenv("AIRFLOW_HOME"):
        db_host = os.getenv("DB_HOST")  # 如果在 Airflow (Docker 容器內) 執行，使用 .env 設定的服務名稱
    else:
        db_host = "127.0.0.1"           # 如果在 Streamlit (VM 宿主機) 執行，透過 127.0.0.1 連接 Docker 映射出來的 Port

    db_port = os.getenv("DB_PORT")
    # 如果有傳入 db_name 就用傳入的，沒有的話預設指向新版的 frontend_db_consol
    target_db = db_name or "frontend_db_consol"
    target_db = quote_plus(target_db)
    uri = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{target_db}?charset=utf8mb4"
    return create_engine(uri, pool_pre_ping=True, pool_recycle=300)

# ===================================================
# 橋接組員資料
# ===================================================
DB_NIGHT_MARKET = "car_accident"
DB_ACCIDENT     = "frontend_db_consol"

# ---------------------------------------------------
# 1. 夜市資料（Night_market_merge）
# 撈取夜市主表，回傳 Pandas DataFrame 以利後續空間運算
# ---------------------------------------------------
def load_night_markets(columns="*"):
    engine = get_db_engine(DB_NIGHT_MARKET)
    query = f"SELECT {columns} FROM Night_market_merge"
    return pd.read_sql(query, engine)

# ---------------------------------------------------
# 2. 事故主表（accident_new_sq1_main）
#    支援：欄位選擇 + 日期篩選
# 動態撈取事故資料，並客製化 tooltip
# ---------------------------------------------------
def load_accidents(columns="*", start_date=None, end_date=None):
    engine = get_db_engine(DB_ACCIDENT)
    query = f"SELECT {columns} FROM accident_new_sq1_main WHERE 1=1"
    if start_date and end_date:
        query += f" AND accident_datetime BETWEEN '{start_date}' AND '{end_date}'"
    df = pd.read_sql(query, engine)
    
    # 計算邏輯：預先組合好 tooltip 文字
    # 在資料撈取階段就先算好地圖所需的 tooltip 字串，可減輕前端 Folium 渲染迴圈時的運算負擔
    # 只做 tooltip（滑鼠滑過才顯示）
    df["tooltip_text"] = df.apply(
        lambda row: (
            f"事故時間：{row['accident_datetime'].strftime('%Y-%m-%d %H:%M')}\n"
            f"死亡：{int(row['death_count'])} 人\n"
            f"受傷：{int(row['injury_count'])} 人"),axis=1)
    return df

# ---------------------------------------------------
# 3. 環境因子（accident_new_sq1_env）
# ---------------------------------------------------
def load_env_factors(columns="*"):
    engine = get_db_engine(DB_ACCIDENT)
    query = f"SELECT {columns} FROM accident_new_sq1_env"
    return pd.read_sql(query, engine)

# ---------------------------------------------------
# 4. 人為因子（accident_new_sq1_process）
# ---------------------------------------------------
def load_human_factors(columns="*"):
    engine = get_db_engine(DB_ACCIDENT)
    query = f"SELECT {columns} FROM accident_new_sq1_process"
    return pd.read_sql(query, engine)
# ---------------------------------------------------

# 資料表檢查工具
# 開發除錯用 - 自動印出資料表的 Schema (欄位與型態)、資料總筆數，以及前 3 筆預覽資料
def inspect_table(engine, db_name, table_name):
    full_table_path = f"`{db_name}`.`{table_name}`"
    print(f"\n{'='*30} Checking Table: {full_table_path} {'='*30}")
    try:
        with engine.connect() as conn:
            _extracted_from_inspect_table_7(full_table_path, conn)
    except Exception as e:
        print(f"Error inspecting {full_table_path}: {e}")

def _extracted_from_inspect_table_7(full_table_path, conn):
    # 1. 型態與索引檢查
    print("[1. Schema Definition]")
    schema = pd.read_sql(text(f"DESC {full_table_path}"), conn)
    print(schema[['Field', 'Type', 'Key']])
    # 2. 筆數統計
    count = conn.execute(text(f"SELECT COUNT(*) FROM {full_table_path}")).scalar()
    print(f"[2. Total Rows: {count:,}]")
    # 3. 資料預覽 (limit 5)
    data = pd.read_sql(text(f"SELECT * FROM {full_table_path} LIMIT 3"), conn)
    print("[3. Data Preview]:")
    if data.empty:
        print(" This table is currently empty.")
    else:
        print(data)

if __name__ == "__main__":
    engine = get_db_engine()
