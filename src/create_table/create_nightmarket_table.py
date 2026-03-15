import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 讀取環境變數
load_dotenv()
username = "root"
password = os.getenv("MYSQL_ROOT_PASSWORD")
server   = "mysql8"
port     = 3306
database = "car_accident"

# 建立資料庫連線
conn_str = f"mysql+pymysql://{username}:{password}@{server}:{port}/{database}?charset=utf8mb4"
engine = create_engine(conn_str)

# 建立資料表 Schema
def create_schema():
    create_sql = """
    CREATE TABLE IF NOT EXISTS Night_market_merge (
        nightmarket_id              VARCHAR(20) NOT NULL UNIQUE,
        nightmarket_name            VARCHAR(30),
        nightmarket_latitude        DECIMAL(10,6),
        nightmarket_longitude       DECIMAL(10,6),
        nightmarket_area_road       VARCHAR(30),
        nightmarket_zipcode         VARCHAR(10),
        nightmarket_zipcode_name    VARCHAR(10),
        nightmarket_rating          FLOAT,
        nightmarket_region          VARCHAR(10),
        nightmarket_city            VARCHAR(10),
        nightmarket_opening_hours   VARCHAR(400),
        nightmarket_url             VARCHAR(200),
        nightmarket_northeast_latitude   DECIMAL(10,6),
        nightmarket_northeast_longitude  DECIMAL(10,6),
        nightmarket_southwest_latitude   DECIMAL(10,6),
        nightmarket_southwest_longitude  DECIMAL(10,6)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()

def main():
    create_schema()

if __name__ == "__main__":
    main()
