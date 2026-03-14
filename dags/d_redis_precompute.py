from datetime import timedelta, datetime, timezone
import os
import time
import math
import uuid
import pandas as pd
import numpy as np  
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import redis
import pickle
import itertools
from dotenv import load_dotenv
from airflow import DAG
from airflow.decorators import task
load_dotenv()

# ==========================================
# 1. 建立資料庫連線
# ==========================================
def get_db_engine():
    # 加入預設值作為保底，防止 .env 讀取失敗時引發 TypeError
    db_user = os.getenv("DB_USER", "root")
    db_pass = quote_plus(os.getenv("DB_PASS", "123456"))
    db_host = os.getenv("DB_HOST", "mysql8")
    db_port = os.getenv("DB_PORT", "3306")
    target_db = "frontend_db_consol"
    
    uri = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{target_db}?charset=utf8mb4"
    return create_engine(uri, pool_pre_ping=True)

# ==========================================
# 2. Redis 連線設定 (連線池架構)
# ==========================================
def get_redis_pool():
    # 在 Docker 內部使用服務名稱 "redis" 互相通訊
    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", 6379))
    password = os.getenv("REDIS_PASSWORD", "123456")
    return redis.ConnectionPool(
        host=host, port=port, password=password if password else None, decode_responses=False)

# 在全域初始化一次連線池，供所有的 Task 共用
REDIS_POOL = get_redis_pool()

def set_cache(key, value, ttl=864000):
    try:
        r = redis.Redis(connection_pool=REDIS_POOL)
        r.setex(key, ttl, pickle.dumps(value))
    except Exception as e:
        print(f"Redis 寫入失敗: {key}, 錯誤: {e}")

def get_empty_result():
    return pd.DataFrame()

# ==========================================
# 3. Airflow DAG 定義區塊
# ==========================================
default_args = {
    "owner": "andrew",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),}

with DAG(
    dag_id='d_redis_precompute',
    default_args=default_args,
    schedule='@monthly',
    start_date=datetime(2026, 3, 4, tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    max_active_runs=1,
    tags=['dashboard', 'redis', 'parallel'],) as dag:

    # ----------------------------------------------------------------
    # Task 1: 計算全台各年總事故量 (支援稽核總表 範圍 A)
    # 直接在 MySQL 中進行聚合，速度最快，不佔用 Airflow 記憶體
    # ----------------------------------------------------------------
    @task
    def precompute_national_yearly_totals():
        engine = get_db_engine()
        sql = """
        SELECT Year, COUNT(accident_id) as total_count 
        FROM frontend_db_consol.tbl_accident_analysis_final 
        GROUP BY Year
        """
        try:
            df_totals = pd.read_sql(sql, engine)
            set_cache("market:audit_national_yearly", df_totals)
            print(f"✅ 全台年度總表 (範圍 A) 計算完成，共 {df_totals['total_count'].sum()} 筆。")
        except Exception as e:
            print(f"全台年度總表計算失敗: {e}")

    # ----------------------------------------------------------------
    # Task 2: 抓取清單，使用「寄物櫃模式」儲存避開 XCom 上限
    # ----------------------------------------------------------------
    @task
    def fetch_and_split_markets() -> list:
        engine = get_db_engine()
        # 夜市清單已經移至 car_accident 庫中
        sql_markets = "SELECT * FROM `car_accident`.`Night_market_merge`"
        try:
            with engine.connect() as conn:
                df_markets = pd.read_sql(sql_markets, conn)
        except Exception as e:
            print(f"無法讀取夜市清單: {e}")
            return []

        valid_markets = []
        for _, row in df_markets.iterrows():
            lat = pd.to_numeric(row['nightmarket_latitude'], errors='coerce')
            lon = pd.to_numeric(row['nightmarket_longitude'], errors='coerce')
            
            if not pd.isna(lat) and not pd.isna(lon):
                valid_markets.append({
                    "name": str(row['nightmarket_name']),
                    "city": str(row['nightmarket_city']),
                    "rating": float(row.get('nightmarket_rating', 0.0)),
                    "lat": float(lat), 
                    "lon": float(lon),
                    "n_lat": float(row.get('nightmarket_northeast_latitude', lat + 0.005)),
                    "s_lat": float(row.get('nightmarket_southwest_latitude', lat - 0.005)),
                    "e_lon": float(row.get('nightmarket_northeast_longitude', lon + 0.005)),
                    "w_lon": float(row.get('nightmarket_southwest_longitude', lon - 0.005))
                })
        
        # 將全台約 300 個夜市切成 10 個批次 (Batch)，準備交給平行運算
        num_batches = 10
        chunk_size = math.ceil(len(valid_markets) / num_batches)
        batches = [valid_markets[i:i + chunk_size] for i in range(0, len(valid_markets), chunk_size)]
        
        # 將龐大資料存入 Redis 置物櫃，只產生極輕量的 String 號碼牌供 XCom 傳遞
        r = redis.Redis(connection_pool=REDIS_POOL)
        run_uuid = str(uuid.uuid4())
        batch_keys = []
        
        for i, batch in enumerate(batches):
            key = f"xcom_claim_check:{run_uuid}:batch_{i}"
            r.setex(key, 86400, pickle.dumps(batch)) # 號碼牌保留 24 小時
            batch_keys.append(key)
            
        print(f"已生成 {len(batch_keys)} 張 Redis 號碼牌。")
        return batch_keys

    # ----------------------------------------------------------------
    # Task 3: 憑「號碼牌」領取批次，執行平行運算 (空間過濾與快取建立)
    # 此 Task 會被 Airflow 的 .expand() 動態擴展為多個 Worker
    # ----------------------------------------------------------------
    @task
    def process_market_batch(batch_key: str):
        r = redis.Redis(connection_pool=REDIS_POOL)
        data = r.get(batch_key)
        if not data:
            return "無法找到對應的Redis資料"
        batch = pickle.loads(data)
        if not batch: return "本批次無資料"

        engine = get_db_engine()
        # 統一維持預熱 3000m 作為最大容器，後端 API 讀取後再進行縮小過濾
        radius_list = [3000] 
        year_targets = ['all_sample']

        with engine.connect() as conn:
            for market in batch:
                lat, lon = market['lat'], market['lon']
                
                # 拉大範圍，用正方形 Bounding Box 向 MySQL 要資料 (3公里約為 3.0/111.0 度)
                max_offset = 3.0 / 111.0 
                params = {"min_lat": lat - max_offset, "max_lat": lat + max_offset, "min_lon": lon - max_offset, "max_lon": lon + max_offset}
                sql_base = text("""
                SELECT accident_id, latitude, longitude, accident_datetime, 
                    death_count, injury_count, primary_cause, party_action,
                    accident_type_major, cause_analysis_major,
                    Year, Hour, weather_condition, light_condition, road_surface_condition
                FROM frontend_db_consol.tbl_accident_analysis_final
                WHERE latitude BETWEEN :min_lat AND :max_lat 
                AND longitude BETWEEN :min_lon AND :max_lon
                """)
                
                try:
                    df_base = pd.read_sql(sql_base, conn, params=params)
                    if not df_base.empty:
                        df_base['latitude'] = pd.to_numeric(df_base['latitude'], errors='coerce')
                        df_base['longitude'] = pd.to_numeric(df_base['longitude'], errors='coerce')
                        df_base['accident_datetime'] = pd.to_datetime(df_base['accident_datetime'], errors='coerce')
                except Exception as e:
                    print(f"資料庫讀取失敗: {e}")
                    continue

                for r_m, y_target in itertools.product(radius_list, year_targets):
                    r_km = r_m / 1000.0
                    cache_key = f"traffic:nearby_v12:{lat:.4f}_{lon:.4f}_{r_km:.1f}_{y_target}"
                    
                    if df_base.empty:
                        set_cache(cache_key, get_empty_result())
                        continue
                        
                    offset = r_km / 111.0
                    mask = (df_base['latitude'].between(lat - offset, lat + offset)) & (df_base['longitude'].between(lon - offset, lon + offset))

                    if y_target != 'all_sample':
                        mask &= (df_base['Year'] == y_target)
                    df_target = df_base[mask]

                    if df_target.empty:
                        set_cache(cache_key, get_empty_result())
                        continue
                    
                    # 丟掉不需要的欄位以節省 Redis 記憶體空間
                    map_columns = [
                        'accident_id', 'accident_datetime', 'Year', 'Hour', 
                        'primary_cause', 'party_action', 'weather_condition', 
                        'light_condition', 'road_surface_condition', 
                        'latitude', 'longitude', 'death_count', 'injury_count',
                        'accident_type_major', 'cause_analysis_major']
                    valid_cols = [c for c in map_columns if c in df_target.columns]
            
                    set_cache(cache_key, df_target[valid_cols])
                time.sleep(0.05) 
        return f"{batch_key} 處理完成"

    # ----------------------------------------------------------------
    # Task 4: 憑所有號碼牌還原全台清單，聚合總表
    # 包含去重邏輯，建立「範圍 B (全台夜市環境不重複事故總數)」
    # ----------------------------------------------------------------
    @task
    def aggregate_national_master(dependency_results, batch_keys: list):
        r = redis.Redis(connection_pool=REDIS_POOL)
        all_dfs = []
        
        # 把各批次的夜市名單全部領出來攤平為一個大陣列
        all_markets = []
        for key in batch_keys:
            data = r.get(key)
            if data:
                all_markets.extend(pickle.loads(data))
        
        # 將剛剛存入 Redis 的各夜市子 DataFrame 讀出來，透過 Concat 聚合成全台大表
        for nm in all_markets:
            lat, lon = nm['lat'], lon = nm['lon']
            key = f"traffic:nearby_v12:{lat:.4f}_{lon:.4f}_3.0_all_sample"
            data = r.get(key)
            
            if data:
                result = pickle.loads(data)
                df = result[0] if isinstance(result, tuple) else result
                
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # Bounding Box 過濾，只保留真的在夜市方框內的事故
                    mask = (df["latitude"].between(nm['s_lat'], nm['n_lat'])) & \
                           (df["longitude"].between(nm['w_lon'], nm['e_lon']))
                    df_strict = df[mask].copy()
                    
                    if not df_strict.empty:
                        # 補上夜市資訊標籤
                        df_strict['nightmarket_name'] = nm['name']
                        df_strict['nightmarket_city'] = str(nm['city']).replace('台', '臺')
                        df_strict['nightmarket_rating'] = float(nm['rating'])
                        all_dfs.append(df_strict)
                        
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            
            # === [新增] 去重處理 (支援稽核表範圍 B) ===
            # 利用 accident_id 剔除因為夜市地理位置重疊而重複計算的事故
            unique_national_accidents = final_df.drop_duplicates(subset=['accident_id'])
            
            # 確保時間特徵完整
            unique_national_accidents['accident_datetime'] = pd.to_datetime(unique_national_accidents['accident_datetime'])
            if "Hour" not in unique_national_accidents.columns:
                unique_national_accidents['Hour'] = unique_national_accidents['accident_datetime'].dt.hour
            unique_national_accidents['Year'] = unique_national_accidents['accident_datetime'].dt.year
            unique_national_accidents['Quarter'] = unique_national_accidents['accident_datetime'].dt.quarter
            unique_national_accidents['Month'] = unique_national_accidents['accident_datetime'].dt.month
            unique_national_accidents['Weekday'] = unique_national_accidents['accident_datetime'].dt.weekday + 1
            
            # === 處理歷史標籤漂移 (Label Drift) 清洗 ===
            unique_national_accidents['accident_type_major'] = unique_national_accidents['accident_type_major'].replace('人與汽(機)車', '人與車')
            unique_national_accidents['accident_type_major'] = unique_national_accidents['accident_type_major'].replace('汽(機車)本身', '車輛本身')
            unique_national_accidents['cause_analysis_major'] = unique_national_accidents['cause_analysis_major'].replace('駕駛人', '駕駛者')

            # === 依據 PDI 定義進行計算 ===
            unique_national_accidents["weight"] = np.where((unique_national_accidents["Hour"] >= 17) | (unique_national_accidents["Hour"] == 0), 1.5, 1.0)
            unique_national_accidents["severity"] = unique_national_accidents["death_count"] * 10 + unique_national_accidents["injury_count"] * 2
            unique_national_accidents["pdi_score"] = unique_national_accidents["severity"] * unique_national_accidents["weight"]
            
            # 將去重後的乾淨總表存入 Redis
            set_cache("market:national_master_df", unique_national_accidents, ttl=864000)
            
            # [新增] 額外存一個總量數字，讓稽核表不用下載整個 DataFrame 就能秒讀總數
            set_cache("market:audit_nightmarket_total_count", len(unique_national_accidents))
            
            print(f"✅ 全台總表聚合完成，去重後共 {len(unique_national_accidents)} 筆獨立事故，已存入 Redis。")
            
            # 任務完成後，清空資料，釋放 Redis 寄物櫃空間
            for key in batch_keys:
                r.delete(key)
        else:
            print("⚠️ 無法聚合全台總表，沒有找到任何快取資料。")

    # ----------------------------------------------------------------
    # 4. 任務執行順序設定
    # ----------------------------------------------------------------
    task_yearly = precompute_national_yearly_totals() # 獨立支線：全台母體計算
    market_batch_keys = fetch_and_split_markets()     # 主線 1：發送號碼牌
    process_tasks = process_market_batch.expand(batch_key=market_batch_keys) # 主線 2：動態擴展平行運算
    aggregate_task = aggregate_national_master(process_tasks, market_batch_keys) # 主線 3：聚合與去重