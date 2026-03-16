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
# 1. 建立 MySQL 資料庫連線
# ==========================================
def get_db_engine():
    db_user = os.getenv("DB_USER")
    db_pass = quote_plus(os.getenv("DB_PASS"))
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    target_db = "frontend_db_consol"
    
    # 組裝連線字串，pool_pre_ping=True 可自動檢查連線是否有效，防止斷線報錯
    uri = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{target_db}?charset=utf8mb4"
    return create_engine(uri, pool_pre_ping=True)

# ==========================================
# 2. 建立 Redis 快取連線 (連線池架構)
# ==========================================
def get_redis_pool():
    # 在 Docker 內部使用服務名稱 "redis" 作為 Host 互相通訊
    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT"))
    password = os.getenv("REDIS_PASSWORD")
    
    # 建立 ConnectionPool (連線池)
    # 好處：後續平行運算取用時能減少重複建立連線的開銷，避免佔用過多系統資源
    return redis.ConnectionPool(
        host=host, port=port, password=password if password else None, decode_responses=False)

# 在全域初始化一次連線池，供所有的 Task 共用
REDIS_POOL = get_redis_pool()

def set_cache(key, value, ttl=864000):
    try:
        # 每次寫入都從共用的 REDIS_POOL 拿一條連線來用
        r = redis.Redis(connection_pool=REDIS_POOL)
        # 使用 pickle.dumps 將 Python 物件打包成二進位格式存入 Redis，ttl 預設保留 10 天
        r.setex(key, ttl, pickle.dumps(value))
    except Exception as e:
        print(f"Redis 寫入失敗: {key}, 錯誤: {e}")
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
    # Task 1: 抓取清單，使用「寄物櫃模式」儲存避開 XCom 上限
    # Airflow 的 Task 之間透過 XCom 傳遞資料，但 XCom 有容量限制
    # 這裡實作「寄物櫃模式」：將龐大的夜市分頁名單(數萬字元)存進 Redis(置物櫃)
    # Task1 只把輕量的 "Redis Key(號碼牌)" 透過 XCom 交給 Task2，避開限制
    # ----------------------------------------------------------------
    @task
    def fetch_and_split_markets() -> list:
        engine = get_db_engine()
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
        
        # 將全台 300 個夜市切成 10 個批次
        num_batches = 10
        chunk_size = math.ceil(len(valid_markets) / num_batches)
        batches = [valid_markets[i:i + chunk_size] for i in range(0, len(valid_markets), chunk_size)]
        
        # 將龐大資料存入 Redis，只產生極輕量的 String 號碼牌
        r = redis.Redis(connection_pool=REDIS_POOL)
        run_uuid = str(uuid.uuid4())
        batch_keys = []
        
        for i, batch in enumerate(batches):
            key = f"xcom_claim_check:{run_uuid}:batch_{i}"
            r.setex(key, 86400, pickle.dumps(batch)) # 號碼牌保留 24 小時
            batch_keys.append(key)
            
        print(f"已生成 {len(batch_keys)} 張 Redis號碼牌。")
        # 回傳的只會是 ['xcom_claim_check:...', ...] 這樣短字串陣列，解決 XCom 爆表問題
        return batch_keys

    # ----------------------------------------------------------------
    # Task 2: 憑「號碼牌」領取批次，執行平行運算
    # 此 Task 會被 Airflow 根據號碼牌的數量自動複製成多個平行的 Worker 同時開工，縮短整體執行時間
    # ----------------------------------------------------------------
    @task
    def process_market_batch(batch_key: str):
        # Redis 提取真正的批次陣列資料
        r = redis.Redis(connection_pool=REDIS_POOL)
        data = r.get(batch_key)
        if not data:
            return "無法找到對應的Redis資料"
        batch = pickle.loads(data)
        if not batch: return "本批次無資料"

        engine = get_db_engine()
        radius_list = [3000] 
        year_targets = ['all_sample']

        with engine.connect() as conn:
            for market in batch:
                lat, lon = market['lat'], market['lon']
                
                # 拉大範圍，用正方形 Bounding Box 向 MySQL 要資料
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

                # 準備不同半徑與年份的組合 (保留了擴充性)
                for r_m, y_target in itertools.product(radius_list, year_targets):
                    r_km = r_m / 1000.0
                    cache_key = f"traffic:nearby_v12:{lat:.4f}_{lon:.4f}_{r_km:.1f}_{y_target}"
                    if df_base.empty:
                        set_cache(cache_key, get_empty_result())
                        continue
                        
                    # 縮小過濾範圍
                    offset = r_km / 111.0
                    mask = (df_base['latitude'].between(lat - offset, lat + offset)) & (df_base['longitude'].between(lon - offset, lon + offset))

                    if y_target != 'all_sample':
                        mask &= (df_base['Year'] == y_target)
                    df_target = df_base[mask]

                    if df_target.empty:
                        set_cache(cache_key, get_empty_result())
                        continue
                    
                    # 丟掉不要的欄位以節省 Redis 記憶體
                    # 這裡存入的是 Pandas DataFrame
                    map_columns = [
                        'accident_id', 'accident_datetime', 'Year', 'Hour', 
                        'primary_cause', 'party_action', 'weather_condition', 
                        'light_condition', 'road_surface_condition', 
                        'latitude', 'longitude', 'death_count', 'injury_count',
                        'accident_type_major', 'cause_analysis_major', 'site_id' ]
                    valid_cols = [c for c in map_columns if c in df_target.columns]
            
                    set_cache(cache_key, df_target[valid_cols])
                time.sleep(0.05) 
        return f"{batch_key} 處理完成"

    # ----------------------------------------------------------------
    # Task 3: 憑所有號碼牌還原全台清單，聚合總表並產出 Audit 輕量報表
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
            lat, lon = nm['lat'], nm['lon']
            key = f"traffic:nearby_v12:{lat:.4f}_{lon:.4f}_3.0_all_sample"
            data = r.get(key)
            
            if data:
                result = pickle.loads(data)
                df = result[0] if isinstance(result, tuple) else result
                
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # Bounding Box 過濾，只保留真的在夜市方框內 (約 500m) 的事故
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
            
            # 確保時間特徵完整
            final_df['accident_datetime'] = pd.to_datetime(final_df['accident_datetime'])
            if "Hour" not in final_df.columns:
                final_df['Hour'] = final_df['accident_datetime'].dt.hour
            final_df['Year'] = final_df['accident_datetime'].dt.year
            final_df['Quarter'] = final_df['accident_datetime'].dt.quarter
            final_df['Month'] = final_df['accident_datetime'].dt.month
            final_df['Weekday'] = final_df['accident_datetime'].dt.weekday + 1
            
            # === 處理歷史標籤漂移 (Label Drift) 清洗 ===
            # 解決長時序資料的標準不一問題
            # 由於警察局填表規範逐年改變，導致同樣的概念出現不同名詞。此處進行正規化統一
            final_df['accident_type_major'] = final_df['accident_type_major'].replace('人與汽(機)車', '人與車')
            final_df['accident_type_major'] = final_df['accident_type_major'].replace('汽(機車)本身', '車輛本身')
            final_df['cause_analysis_major'] = final_df['cause_analysis_major'].replace('駕駛人', '駕駛者')

            # === 依據最新簡報定義 PDI ===
            final_df["weight"] = np.where((final_df["Hour"] >= 17) | (final_df["Hour"] == 0), 1.5, 1.0)
            final_df["severity"] = final_df["death_count"] * 10 + final_df["injury_count"] * 2
            final_df["pdi_score"] = final_df["severity"] * final_df["weight"]
            
            # 存入給其他圖表用的原始巨型 DataFrame
            set_cache("market:national_master_df", final_df, ttl=864000)
            print(f"✅ 全台夜市周邊總表聚合完成，共 {len(final_df)} 筆精準事故，已存入 Redis。")

            # ========================================================
            # 前端 Audit 儀表板設計的「輕量化數字」 
            # 運算完全在記憶體中進行，產出極小的字典，不會增加 Redis 負擔
            # ========================================================
            
            # 1. 新增前端需要的「白天/夜間」時段標籤 (06-18為白天)
            final_df['time_slot'] = final_df['Hour'].apply(lambda x: 'Day' if 6 <= x < 18 else 'Night')

            # 定義共用的聚合函數 (只算總量與 PDI 總和)
            def generate_stats(df_target, groupby_cols):
                res = df_target.groupby(groupby_cols).agg(
                    acc_count=('accident_id', 'count'), # 計算總事故數
                    pdi_total=('pdi_score', 'sum')      # 加總剛算好的 PDI
                ).reset_index()
                return res.to_dict('records')

            # 2. 巨觀統計 (Macro)：全台夜市周邊總計、各縣市夜市周邊總計
            # 這裡的「全台」與「各縣市」是指「發生在所有夜市 500m 範圍內的加總」，不包含非夜市區域
            taiwan_market_stats = generate_stats(final_df, ['Year', 'Quarter', 'Month', 'time_slot'])
            city_market_stats = generate_stats(final_df, ['nightmarket_city', 'Year', 'Quarter', 'Month', 'time_slot'])
            
            macro_bundle = {
                "taiwan_markets_total": taiwan_market_stats,
                "city_markets_total": city_market_stats,
                "updated_at": str(datetime.now())
            }
            # 將巨觀數字打包存入一個專屬的 Key
            set_cache("traffic:stats:audit_macro", macro_bundle)

            # 3. 微觀統計 (Micro)：單一特定夜市 500m 總計
            # 將 300 個夜市分開存成各自的 Key，讓前端地圖點擊時可以「秒拉」資料
            market_groups = final_df.groupby('nightmarket_name')
            for m_name, m_df in market_groups:
                m_stats = generate_stats(m_df, ['Year', 'Quarter', 'Month', 'time_slot'])
                set_cache(f"traffic:stats:audit_market:{m_name}", m_stats)

            print("✅ Audit 儀表板輕量化統計運算完成，已存入 Redis。")
            
            # 任務完成後，清空資料，釋放 Redis 寄物櫃空間
            for key in batch_keys:
                r.delete(key)
        else:
            print("⚠️ 無法聚合全台總表，沒有找到任何快取資料。")

    # ----------------------------------------------------------------
    # 4. 任務執行順序設定
    # ----------------------------------------------------------------
    market_batch_keys = fetch_and_split_markets() 
    process_tasks = process_market_batch.expand(batch_key=market_batch_keys)  
    aggregate_task = aggregate_national_master(process_tasks, market_batch_keys)