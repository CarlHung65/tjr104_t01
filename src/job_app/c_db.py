import os
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 解決欄位顯示不完整問題: 確保能清楚看到所有欄位
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
load_dotenv()

def get_db_engine():
    user = quote_plus(os.getenv("user"))
    passwd = quote_plus(os.getenv("passwd"))
    host = os.getenv("host")
    port = os.getenv("port")
    db_name = "test_accident" # 預設連線至 test_accident，查詢時會使用完整的"db"."table"路徑
    uri = f"mysql+pymysql://{user}:{passwd}@{host}:{port}/{db_name}?charset=utf8mb4"
    return create_engine(uri, pool_pre_ping=True, pool_recycle=300)

def inspect_table(engine, db_name, table_name):
    full_table_path = f"`{db_name}`.`{table_name}`"
    print(f"\n{'='*30} Checking Table: {full_table_path} {'='*30}")
    try:
        with engine.connect() as conn:
            # 1. 型態與索引檢查
            print("[1. Schema Definition]")
            schema = pd.read_sql(text(f"DESC {full_table_path}"), conn)
            print(schema[['Field', 'Type', 'Key']])
            
            # 2. 筆數統計
            count = conn.execute(text(f"SELECT COUNT(*) FROM {full_table_path}")).scalar()
            print(f"[2. Total Rows: {count:,}]")
            
            # 3. 資料預覽 (limit 5)
            data = pd.read_sql(text(f"SELECT * FROM {full_table_path} LIMIT 5"), conn)
            print("[3. Data Preview]:")
            if data.empty:
                print(" This table is currently empty.")
            else:
                print(data)
    except Exception as e:
        print(f"Error inspecting {full_table_path}: {e}")

if __name__ == "__main__":
    engine = get_db_engine()
    
    # 所有資料表型態與內容
    target_tables = [
        # test_accident 核心表 (10張)
        ("test_accident", "accident_sq1_env"),
        ("test_accident", "accident_sq1_human"),
        ("test_accident", "accident_sq1_main"),
        ("test_accident", "accident_sq1_process"),
        ("test_accident", "accident_sq1_res"),
        ("test_accident", "accident_sq2_env"),
        ("test_accident", "accident_sq2_human"),
        ("test_accident", "accident_sq2_process"),
        ("test_accident", "accident_sq2_res"),
        ("test_accident", "accident_sq2_sub"),
        
        # test_night_market (1張)
        ("test_night_market", "Night_market_merge"),
        
        # test_weather (3張)
        ("test_weather", "accident_nearby_obs_stn"),
        ("test_weather", "obs_stations"),
        ("test_weather", "weather_hourly_history")
    ]
    
    for db, table in target_tables:
        inspect_table(engine, db, table)