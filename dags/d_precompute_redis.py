from datetime import timedelta, datetime, timezone
import os
import time
import math
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import redis
import pickle
import itertools
from dotenv import load_dotenv
from airflow.sdk import DAG, task

# 載入環境變數
load_dotenv()

# ==========================================
# 1. 連線設定
# ==========================================
def get_db_engine():
    """取得 MySQL 資料庫連線引擎"""
    cloud_sql_url = os.getenv("CLOUDSQL_URL")
    if cloud_sql_url:
        # 注意：若在 Docker 執行且 SQL 在 VM 本機，需將 URL 中的 127.0.0.1 換成 VM 內網 IP
        return create_engine(cloud_sql_url, pool_pre_ping=True)

redis_host = os.getenv("REDIS_HOST") or ( "redis" if os.getenv("AIRFLOW_HOME") else "localhost" )
REDIS_POOL = redis.ConnectionPool(
    host=redis_host,
    port=int(os.getenv("REDIS_PORT")),
    password=os.getenv("REDIS_PASSWORD"),
    decode_responses=False)

def set_cache(key, value, ttl=172800):
    """將運算結果壓縮並寫入 Redis"""
    try:
        r = redis.Redis(connection_pool=REDIS_POOL)
        r.setex(key, ttl, pickle.dumps(value))
    except Exception as e:
        print(f"Redis 寫入失敗: {key}, 錯誤: {e}")

def get_empty_result():
    """回傳空資料的標準格式，避免程式碼重複"""
    return (pd.DataFrame(), {"total":0, "dead":0, "hurt":0}, {}, pd.DataFrame())

# ==========================================
# 2. Airflow DAG 定義區塊
# ==========================================
default_args = {
    "owner": "andrew",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id='d_dashboard_redis_precompute_parallel',
    default_args=default_args,
    # 將原本的字串改為 timedelta，這樣就是每 15 秒跑一次，且不會有語法錯誤
    schedule='0 2 * * *', 
    start_date=datetime(2026, 3, 4, tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    max_active_runs=1, # 這個很重要！每15秒跑一次時，確保前一個沒跑完時，第二個不會重疊啟動
    tags=['dashboard', 'redis', 'parallel'],
) as dag:

    # ----------------------------------------------------------------
    # Task 1: 負責去資料庫撈取所有夜市，並切分成 10 份 (Batches)
    # 使用 @task 裝飾器，這是 Airflow TaskFlow API 的標準寫法
    # ----------------------------------------------------------------
    @task
    def fetch_and_split_markets() -> list:
        engine = get_db_engine()
        sql_markets = "SELECT nightmarket_latitude, nightmarket_longitude FROM `test_night_market`.`Night_market_merge`"
        
        try:
            with engine.connect() as conn:
                df_markets = pd.read_sql(sql_markets, conn)
        except Exception as e:
            print(f"無法讀取夜市清單: {e}")
            return []

        # 過濾掉沒有座標的髒資料，並轉成 Dictionary 列表
        # 業界實務：不在 Task 之間傳遞龐大的 DataFrame，只傳遞輕量的 Dict 或 List，以避免塞爆 Airflow 的 XCom 資料庫
        valid_markets = []
        for _, row in df_markets.iterrows():
            lat = pd.to_numeric(row['nightmarket_latitude'], errors='coerce')
            lon = pd.to_numeric(row['nightmarket_longitude'], errors='coerce')
            if not pd.isna(lat) and not pd.isna(lon):
                valid_markets.append({"lat": lat, "lon": lon})
        
        # 將資料等分成 10 份
        num_batches = 10
        chunk_size = math.ceil(len(valid_markets) / num_batches)
        batches = [valid_markets[i:i + chunk_size] for i in range(0, len(valid_markets), chunk_size)]
        
        print(f"總共找到 {len(valid_markets)} 個夜市，已切分為 {len(batches)} 個批次。")
        return batches # 這個回傳值會自動存入 XCom，傳遞給下一個任務

    # ----------------------------------------------------------------
    # Task 2: 負責接收「其中 1 份」夜市清單，並進行高強度 Pandas 運算
    # ----------------------------------------------------------------
    @task
    def process_market_batch(batch: list):
        if not batch:
            return "本批次無資料"
            
        engine = get_db_engine()
        radius_list = [3000]
        ## radius_list = [500, 1000, 1500, 2000, 2500, 3000]
        year_targets = ['all_sample']

        with engine.connect() as conn:
            for market in batch:
                lat, lon = market['lat'], market['lon']
                
                # 1. 一次性撈取該夜市最大範圍 (3000m) 的資料
                max_offset = 3.0 / 111.0 
                params = {"min_lat": lat - max_offset, "max_lat": lat + max_offset, "min_lon": lon - max_offset, "max_lon": lon + max_offset}
                

                sql_base = text("""
                    SELECT a.accident_id, a.accident_datetime, a.latitude, a.longitude, 
                        a.death_count, a.injury_count, a.primary_cause, a.Year, a.Hour, a.Weekday_CN,
                        m.weather_condition,
                        CASE 
                            WHEN a.Hour >= 6 AND a.Hour < 12 THEN '早'
                            WHEN a.Hour >= 12 AND a.Hour < 18 THEN '午'
                            ELSE '晚'
                        END AS Period
                    FROM test_accident.tbl_accident_analysis a
                    LEFT JOIN test_accident.accident_sq1_main m ON a.accident_id = m.accident_id
                    WHERE a.latitude BETWEEN :min_lat AND :max_lat 
                    AND a.longitude BETWEEN :min_lon AND :max_lon
                """)
                
                try:
                    df_base = pd.read_sql(sql_base, conn, params=params)
                    if not df_base.empty:
                        df_base['latitude'] = pd.to_numeric(df_base['latitude'], errors='coerce')
                        df_base['longitude'] = pd.to_numeric(df_base['longitude'], errors='coerce')
                        df_base['accident_datetime'] = pd.to_datetime(df_base['accident_datetime'], errors='coerce')
                    #   df_base['Year'] = df_base['accident_datetime'].dt.year # 預先萃取年份
                except Exception as e:
                    print(f"資料庫讀取失敗 ({lat}, {lon}): {e}")
                    continue

                # 2. 使用 itertools 將 6個半徑 x 5個年份 攤平成一維迴圈，提升效能
                for r_m, y_target in itertools.product(radius_list, year_targets):
                    r_km = r_m / 1000.0
                    cache_key = f"traffic:nearby_v12:{lat:.4f}_{lon:.4f}_{r_km:.1f}_{y_target}"
                    
                    if df_base.empty:
                        set_cache(cache_key, get_empty_result())
                        continue

                    # 3. 使用 Pandas 向量化篩選空間與年份
                    offset = r_km / 111.0
                    mask = (df_base['latitude'].between(lat - offset, lat + offset)) & (df_base['longitude'].between(lon - offset, lon + offset))
                    if y_target != 'all_sample':
                        mask &= (df_base['Year'] == y_target)
                    
                    df_target = df_base[mask]
                    
                    if df_target.empty:
                        set_cache(cache_key, get_empty_result())
                        continue
                    
                    # 4. 緊湊化的指標與圖表運算
                    stats = {"total": len(df_target), "dead": int(df_target['death_count'].sum()), "hurt": int(df_target['injury_count'].sum())}
                    charts = {
                        'top10': df_target['primary_cause'].value_counts().head(10).rename_axis('肇因').reset_index(name='件數'),
                        'hour': df_target['Hour'].value_counts().sort_index().rename_axis('Hour').reset_index(name='件數'),
                        'weather': df_target.groupby('weather_condition').agg(件數=('accident_id','count'), 死亡=('death_count','sum'), 受傷=('injury_count','sum')).rename_axis('天氣').reset_index()
                    }
                    # map_df = df_target.nlargest(1000, ['death_count', 'injury_count', 'accident_datetime'])[['accident_datetime', 'latitude', 'longitude', 'death_count', 'injury_count', 'primary_cause', 'Year', 'Hour', 'weather_condition']]
                    set_cache(cache_key, (df_target, stats, charts, pd.DataFrame()))

                    # set_cache(cache_key, (map_df, stats, charts, pd.DataFrame()))
                
                print(f"完成夜市計算: {round(lat,4)}, {round(lon,4)}")
                time.sleep(0.05) # 批次內仍保留微小延遲，避免瞬間 CPU 飆升

        return f"本批次 {len(batch)} 個夜市處理完成"

    # ----------------------------------------------------------------
    # 任務依賴與動態映射設定 (Dynamic Task Mapping)
    # ----------------------------------------------------------------
    # 1. 取得切分好的 10 份名單
    market_batches = fetch_and_split_markets()
    
    # 2. .expand() 是平行運算的靈魂。
    # 根據 market_batches 清單的長度 (10)，自動複製出 10 個 process_market_batch 任務同時平行執行。
    process_market_batch.expand(batch=market_batches)