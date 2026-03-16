from datetime import timedelta, datetime, timezone
import os
import pandas as pd
import redis
import pickle
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
from airflow import DAG
from airflow.sdk import task # 使用最新版 SDK 導入方式

load_dotenv()

# --- 基礎連線設定 ---
def get_db_engine():
    db_user, db_pass = os.getenv("DB_USER"), quote_plus(os.getenv("DB_PASS"))
    db_host, db_port = os.getenv("DB_HOST"), os.getenv("DB_PORT")
    uri = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/car_accident?charset=utf8mb4"
    return create_engine(uri, pool_pre_ping=True)

def get_redis_client():
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"), 
        port=int(os.getenv("REDIS_PORT", 6379)), 
        password=os.getenv("REDIS_PASSWORD")
    )

default_args = {'owner': 'andrew', 'retries': 1, 'retry_delay': timedelta(minutes=5)}

with DAG(
    dag_id='d_redis_audit',
    default_args=default_args,
    schedule='@monthly',
    start_date=datetime(2026, 3, 1, tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=['dashboard', 'precompute', 'redis']
) as dag:

    #### 1. 抓取全量數據與時段分類
    @task
    def fetch_base_data():
        engine = get_db_engine()
        # 僅選取必要欄位，並直接計算 PDI
        sql = """
        SELECT nightmarket_city as city, Year, 
               QUARTER(accident_datetime) as Quarter, 
               MONTH(accident_datetime) as Month,
               Hour, (death_count + injury_count) as pdi,
               latitude, longitude
        FROM Night_market_merge
        """
        df = pd.read_sql(sql, engine)
        # 時段分類邏輯：06-18 為白天，其餘為夜間
        df['time_slot'] = df['Hour'].apply(lambda x: 'Day' if 6 <= x < 18 else 'Night')
        return df

    #### 2. 產出全台與縣市統計 (Macro)
    @task
    def compute_macro_stats(df: pd.DataFrame):
        # 分別計算：總事故量、總 PDI
        def aggregate_data(target_df, group_cols):
            res = target_df.groupby(group_cols).agg(
                acc_count=('Year', 'count'),
                pdi_total=('pdi', 'sum')
            ).reset_index()
            return res.to_dict('records')

        # 顆粒度：全台-時段-年月季 / 縣市-時段-年月季
        taiwan_stats = aggregate_data(df, ['Year', 'Quarter', 'Month', 'time_slot'])
        city_stats = aggregate_data(df, ['city', 'Year', 'Quarter', 'Month', 'time_slot'])
        
        bundle = {"taiwan": taiwan_stats, "cities": city_stats, "updated_at": str(datetime.now())}
        r = get_redis_client()
        r.set("traffic:stats:macro_summary", pickle.dumps(bundle))
        return "Macro stats stored"

    #### 3. 產出夜市 500m 統計 (Micro)
    @task
    def compute_market_stats(df: pd.DataFrame):
        engine = get_db_engine()
        markets = pd.read_sql("SELECT nightmarket_name, nightmarket_city, nightmarket_latitude as lat, nightmarket_longitude as lon FROM `car_accident`.`Night_market_merge`", engine)
        r_client = get_redis_client()
        
        for _, m in markets.iterrows():
            # 500m 空間過濾 (約 0.0045 度)
            m_lat, m_lon = float(m['lat']), float(m['lon'])
            offset = 0.5 / 111.0
            mask = (df['latitude'].between(m_lat-offset, m_lat+offset)) & (df['longitude'].between(m_lon-offset, m_lon+offset))
            m_df = df[mask]
            
            if m_df.empty:
                r_client.set(f"traffic:stats:market:{m['nightmarket_name']}", pickle.dumps({"stats": [], "market_info": m.to_dict()}))
                continue
                
            # 計算夜市年月季統計
            stats = m_df.groupby(['Year', 'Quarter', 'Month', 'time_slot']).agg(
                acc_count=('Year', 'count'),
                pdi_total=('pdi', 'sum')
            ).reset_index().to_dict('records')
            
            r_client.set(f"traffic:stats:market:{m['nightmarket_name']}", pickle.dumps({"stats": stats, "market_info": m.to_dict()}))
        return "Market stats stored"

    # 設定執行順序
    base_df = fetch_base_data()
    compute_macro_stats(base_df) >> compute_market_stats(base_df)