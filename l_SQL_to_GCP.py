import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 讀取環境變數
load_dotenv()
username = os.getenv("GCP_username")
password = os.getenv("GCP_password")
server   = "localhost"
port     = 3307
database = "test_night_market"

# 建立資料庫連線
conn_str = f"mysql+pymysql://{username}:{password}@{server}:{port}/{database}?charset=utf8mb4"
engine = create_engine(conn_str)

# 讀取 CSV
def load_csv():
    csv_path = r".\Data_clean\nightmarket_clean.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"找不到 CSV 檔案：{csv_path}")

    dtype_dict = {
        'nightmarket_id': str,
        'nightmarket_weekday': str
    }
    df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=dtype_dict)
    return df

# 建立資料表 Schema
def create_schema():
    create_sql = """
    CREATE TABLE IF NOT EXISTS Night_market_merge (
        nightmarket_id              VARCHAR(20) NOT NULL UNIQUE,
        nightmarket_name            VARCHAR(30),
        nightmarket_latitude        DECIMAL(15,4),
        nightmarket_longitude       DECIMAL(15,4),
        nightmarket_area_road       VARCHAR(30),
        nightmarket_zipcode         VARCHAR(10),
        nightmarket_zipcode_name    VARCHAR(10),
        nightmarket_rating          FLOAT,
        nightmarket_region          VARCHAR(10),
        nightmarket_city            VARCHAR(10),
        nightmarket_opening_hours   VARCHAR(400),
        nightmarket_url             VARCHAR(200),
        nightmarket_northeast_latitude   DECIMAL(15,4),
        nightmarket_northeast_longitude  DECIMAL(15,4),
        nightmarket_southwest_latitude   DECIMAL(15,4),
        nightmarket_southwest_longitude  DECIMAL(15,4)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()

# 主程式
def main():
    try:
        df = load_csv()
        create_schema()

        # 清空目標表格 Night_market_merge
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE Night_market_merge"))
            conn.commit()

        # 欄位型態轉換
        numeric_cols = [
            'nightmarket_latitude', 'nightmarket_longitude',
            'nightmarket_northeast_latitude', 'nightmarket_northeast_longitude',
            'nightmarket_southwest_latitude', 'nightmarket_southwest_longitude',
            'nightmarket_rating'
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # 排序欄位
        columns_in_db = [
            'nightmarket_id', 'nightmarket_name', 'nightmarket_latitude', 'nightmarket_longitude',
            'nightmarket_area_road', 'nightmarket_zipcode', 'nightmarket_zipcode_name',
            'nightmarket_rating', 'nightmarket_region', 'nightmarket_city',
            'nightmarket_url','nightmarket_northeast_latitude', 'nightmarket_northeast_longitude',
            'nightmarket_southwest_latitude', 'nightmarket_southwest_longitude', "nightmarket_opening_hours"
        ]
        df = df[columns_in_db]

        # 寫入 Night_market_merge 表格
        df.to_sql(
            name="Night_market_merge",
            con=engine,
            if_exists="append",
            index=False,
            method='multi'
        )

        print("Night_market_merge 表格資料新增完成")

    except Exception as e:
        print(f"❌ 發生錯誤：{e}")

if __name__ == "__main__":
    main()


# 【 l_SQL_to_GCP V1.1版 】

# / 程式功能
# ETL（Extract → Transform → Load）流程：
# Extract
# 從 CSV 讀取清理好的夜市資料。
# Transform
# 欄位型別轉換
# 整理欄位順序
# 確保資料格式統一
# Load
# 建立資料表
# 清空舊資料
# 寫入 MySQL（Night_market_merge）