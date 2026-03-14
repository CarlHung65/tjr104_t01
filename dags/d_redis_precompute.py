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

# 載入環境變數
load_dotenv()

# ==========================================
# 1. 智慧連線設定 (自動連結 SSH 隧道)
# 路拓樸：為了解決 Airflow 容器無法直接連到跳板機的問題
# 函式會自動尋找 host.docker.internal 或是掃描 Docker 網段 (172.x.x.x)，智慧建立連線
# ==========================================
def get_db_engine():
    """
    取得 MySQL 資料庫連線引擎
    自動掃描所有可能的 Docker 閘道 IP，尋找使用者在本機建立好的 gcloud SSH 3308 隧道。
    """
    user = "root" 
    passwd = quote_plus(os.getenv("MYSQL_PASSWORD"))
    db = os.getenv("MYSQL_DATABASE")
    port = 3308 
    
    candidate_hosts = []
    if os.getenv("AIRFLOW_HOME"):
        candidate_hosts.append("host.docker.internal")
        try:
            import subprocess
            res = subprocess.run(["ip", "route"], capture_output=True, text=True)
            for line in res.stdout.split('\n'):
                if 'default' in line:
                    candidate_hosts.append(line.split(' ')[2])
        except:
            pass
        candidate_hosts.extend(["172.17.0.1", "172.18.0.1", "172.19.0.1", "172.20.0.1", "172.21.0.1"])
    else:
        candidate_hosts.append("127.0.0.1")
        
    last_err = None
    for host in candidate_hosts:
        uri = f"mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}?charset=utf8mb4"
        try:
            # 設定 connect_timeout=10，給 SSH 隧道足夠的首次連線時間
            engine = create_engine(uri, pool_pre_ping=True, connect_args={'connect_timeout': 10})
            with engine.connect(): 
                print(f"✅ 成功透過 {host}:{port} 連線至 MySQL")
                return engine
        except Exception as e:
            last_err = e
            continue
            
    raise ConnectionError(f"❌ 無法連線至跳板機 MySQL。請確認 gcloud tunnel 正在執行中。最後錯誤: {last_err}")

# ==========================================
# 2. Redis 快取設定
# ==========================================
def get_redis_pool():
    host = "redis" if os.getenv("AIRFLOW_HOME") else os.getenv("REDIS_HOST", "127.0.0.1")
    port = int(os.getenv("REDIS_PORT", 6379))
    password = os.getenv("REDIS_PASSWORD", "")
    return redis.ConnectionPool(
        host=host, port=port, password=password if password else None, decode_responses=False)

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
    dag_id='d_precompute_redis',
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
        sql_markets = "SELECT * FROM `test_night_market`.`Night_market_merge`"
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
        
        # 將全台 300 個夜市切成 10 個批次 (Batch)
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
    # Task 3: 憑所有號碼牌還原全台清單，聚合總表
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
                        
            set_cache("market:national_master_df", final_df, ttl=864000)
            print(f"✅ 全台總表聚合完成，共 {len(final_df)} 筆精準事故，已存入 Redis。")
            
            # 任務完成後，清空資料，釋放 Redis 寄物櫃空間
            for key in batch_keys:
                r.delete(key)
        else:
            print("⚠️ 無法聚合全台總表，沒有找到任何快取資料。")

    # ----------------------------------------------------------------
    # 4. 任務執行順序設定
    # ----------------------------------------------------------------
    market_batch_keys = fetch_and_split_markets() # market_batch_keys 現在裝的是 ['xcom_claim_check:uuid:batch_0', ...]
    process_tasks = process_market_batch.expand(batch_key=market_batch_keys)  # Airflow 將 10 個短字串派發給 10 個平行任務
    aggregate_task = aggregate_national_master(process_tasks, market_batch_keys)