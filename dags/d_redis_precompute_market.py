from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import redis
import pickle
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
# ==========================================
# 建立資料庫連線
# ==========================================
def get_db_engine():
    db_user = os.getenv("DB_USER")
    db_pass = quote_plus(os.getenv("DB_PASS"))
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    target_db = "frontend_db_consol"
    
    uri = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{target_db}?charset=utf8mb4"
    return create_engine(uri, pool_pre_ping=True)

def load_night_markets():
    engine = get_db_engine()
    return pd.read_sql("SELECT * FROM `car_accident`.`Night_market_merge`", engine)

# ==========================================
# Redis 連線設定
# ==========================================
def get_redis_pool():
    # 在新 VM Docker 環境下，Host 直接使用服務名稱 "redis"
    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT"))
    password = os.getenv("REDIS_PASSWORD")
    return redis.ConnectionPool(
        host=host, port=port, password=password if password else None, decode_responses=False)

# 在全域初始化一次即可，後續所有任務共用此池
REDIS_POOL = get_redis_pool()

def get_redis_client():
    # 每次需要操作 Redis 時從池子裡拿一個連線出來
    return redis.Redis(connection_pool=REDIS_POOL)

# 在全域初始化一次即可，後續所有任務共用此連線
r = get_redis_client()

# 預先計算夜市統計數據
# 主要 Python 任務 - 空間運算與指標聚合
def precompute_market_stats():
    nm_df = load_night_markets()
    engine = get_db_engine()
    
    # 定義要獨立計算的目標：先算全部，再依序算各個年份
    target_years = ['all', 2026, 2025, 2024, 2023, 2022, 2021]
    
    # 用於儲存 Act3 行人安全導航的字典
    act3_guides = {nm["nightmarket_name"].strip(): {"df_list": []} for _, nm in nm_df.iterrows()}
    
    # 迴圈：一年年獨立向資料庫查詢，算完就清空記憶體
    for target_year in target_years:
        print(f"開始處理年份: {target_year}")
        
        # 建立該年份專屬的計分板
        stats_board = {nm["nightmarket_name"].strip(): {"count": 0, "pdi": 0.0, "dead": 0, "hurt": 0} for _, nm in nm_df.iterrows()}
        heatmap_list = []
        
        # 根據年份動態組裝 SQL 語法，把篩選壓力交給資料庫
        if target_year == 'all':
            query = "SELECT latitude, longitude, death_count, injury_count, accident_datetime, weather_condition, Hour FROM `frontend_db_consol`.`tbl_accident_analysis_final`"
        else:
            query = f"SELECT latitude, longitude, death_count, injury_count, accident_datetime, weather_condition, Hour FROM `frontend_db_consol`.`tbl_accident_analysis_final` WHERE Year = {target_year}"
            
        # 效能優化：Chunksize 批次讀取
        # 針對該年份，每次只拿5萬筆進來算，算完就換下一批新的5萬筆，避免幾十萬筆資料一次塞爆記憶體
        for chunk in pd.read_sql(query, engine, chunksize=50000):
            chunk["accident_datetime"] = pd.to_datetime(chunk["accident_datetime"])

            # 向量化計算 PDI
            # 晚上 5 點到凌晨 12 點前，權重為 1.5，其餘為 1。死亡 10 分，受傷 2 分。
            # 利用 numpy 的 np.where 進行整柱 (Column) 運算，極度快速。
            chunk["weight"] = np.where((chunk["Hour"] >= 17) | (chunk["Hour"] == 0), 1.5, 1)
            chunk["severity"] = chunk["death_count"] * 10 + chunk["injury_count"] * 2
            chunk["pdi_score"] = chunk["severity"] * chunk["weight"]
            
            # 效能優化：將 Pandas Series 轉為 Numpy Array 以加速迴圈內的碰撞計算
            lats, lons = chunk["latitude"].values, chunk["longitude"].values
            scores = chunk["pdi_score"].values
            deaths = chunk["death_count"].values
            hurts = chunk["injury_count"].values
            
            for _, nm in nm_df.iterrows():
                name = nm["nightmarket_name"].strip()
                # 空間過濾
                mask = (lats >= nm["nightmarket_southwest_latitude"]) & (lats <= nm["nightmarket_northeast_latitude"]) & \
                       (lons >= nm["nightmarket_southwest_longitude"]) & (lons <= nm["nightmarket_northeast_longitude"])
                
                if mask.any():
                    # 用 boolean mask 快速對符合條件的列進行加總
                    stats_board[name]["count"] += int(mask.sum())
                    stats_board[name]["pdi"] += float(scores[mask].sum())
                    stats_board[name]["dead"] += int(deaths[mask].sum()) # 累加死亡人數
                    stats_board[name]["hurt"] += int(hurts[mask].sum())  # 累加受傷人數              
                    # 如果是'all'年份，把落在夜市的事故存起來，稍後用來算 Act3
                    if target_year == 'all':
                        act3_guides[name]["df_list"].append(chunk[mask])
            valid_heat = chunk[chunk["severity"] > 0]
            heatmap_list.extend(valid_heat[["latitude", "longitude", "pdi_score"]].values.tolist())
            
        # 該年份的資料庫批次讀取結束，準備轉換格式
        final_results = []
        for _, nm in nm_df.iterrows():
            name = nm["nightmarket_name"].strip()
            final_results.append({
                "nightmarket_id": nm.get("nightmarket_id", ""),
                "nightmarket_name": name,
                "nightmarket_city": nm.get("nightmarket_city", ""),
                "nightmarket_rating": float(nm.get("nightmarket_rating", 0.0)),
                "nightmarket_url": nm.get("nightmarket_url", ""),
                "accident_count": stats_board[name]["count"],
                "death_count": stats_board.get(name, {}).get("dead", 0),
                "injury_count": stats_board.get(name, {}).get("hurt", 0),
                "pdi": stats_board[name]["pdi"]
            })
            
        # ====================================================
        # 效能保護：熱力圖抽樣與記憶體微縮
        # ====================================================
        # 將全台熱點縮減至 3500 點以內，兼顧視覺分佈並大幅減輕 Redis 記憶體壓力
        if len(heatmap_list) > 3500:
            import random
            heatmap_list = random.sample(heatmap_list, 3500)
        
        # 捨去不必要的浮點數長度：經緯度取 4 位 (精度約 11 公尺)、分數取 1 位，讓 JSON/Pickle 體積縮小至少一半
        heatmap_list = [[round(p[0], 4), round(p[1], 4), round(p[2], 1)] for p in heatmap_list]
            
        # 算完一個年份，立刻存入 Redis
        key_suffix = str(target_year)
        
        # 【防呆機制】寫入前先主動刪除舊的 Key，強迫 Redis 提前釋放舊記憶體區塊，防止 OOM
        r.delete(f"market:pdi_stats_cache_{key_suffix}")
        r.delete(f"traffic:global_heatmap_cache_{key_suffix}")
        
        r.set(f"market:pdi_stats_cache_{key_suffix}", pickle.dumps(final_results), ex=864000)
        r.set(f"traffic:global_heatmap_cache_{key_suffix}", pickle.dumps(heatmap_list), ex=864000)
        print(f"年份 {target_year} 已成功寫入 Redis！")

    # ====================================================
    # 產出 Act 3 行人步行建議導航 (等年份跑完後，一次結算)
    # 分析單一夜市的各項風險特徵
    # ====================================================

    print("各年份統計已完成。開始計算 Act3 行人步行建議導航...")
    final_act3_guides = {}
    for _, nm in nm_df.iterrows():
        name = nm["nightmarket_name"].strip()
        final_act3_guides[name] = None
        
        if act3_guides[name]["df_list"]:
            df_tight = pd.concat(act3_guides[name]["df_list"], ignore_index=True)
   
            # 1. 天氣與危險時段
            rain_ratio = (df_tight['weather_condition'].fillna('').str.contains('雨').sum() / len(df_tight)) * 100
            peak_hour = df_tight['Hour'].value_counts().idxmax() if not df_tight.empty else 20
            peak_period = "20–22 時" if 20 <= peak_hour <= 22 else f"{peak_hour}:00 時段"
            
            # 2. 區域風險方位
            # 幾何計算：以夜市中心點為十字座標，區分東、西、南、北側的事故數量，找出最危險的象限
            c_lat, c_lon = nm["nightmarket_latitude"], nm["nightmarket_longitude"]
            zone_map = {
                "北側": df_tight[df_tight["latitude"] > c_lat].shape[0],
                "南側": df_tight[df_tight["latitude"] < c_lat].shape[0],
                "東側": df_tight[df_tight["longitude"] > c_lon].shape[0],
                "西側": df_tight[df_tight["longitude"] < c_lon].shape[0],}
            danger_zone = max(zone_map, key=zone_map.get)
            
            # 3. 最安全入口推薦 (尋找離事故群最遠的點)
            north, south = nm["nightmarket_northeast_latitude"], nm["nightmarket_southwest_latitude"]
            east, west = nm["nightmarket_northeast_longitude"], nm["nightmarket_southwest_longitude"]
            
            # 定義夜市的 8 個潛在入口(四個邊的中心 + 四個角落)
            candidates = {
                "北側入口": [north, (west + east) / 2], "南側入口": [south, (west + east) / 2],
                "東側入口": [(south + north) / 2, east], "西側入口": [(south + north) / 2, west],
                "東北角入口": [north, east], "西北角入口": [north, west],
                "東南角入口": [south, east], "西南角入口": [south, west],}
            
            # 計算每個入口到所有「事故點」的最短距離
            # 選出「最短距離中的最大值」，代表該入口離所有危險點最遠
            # 修改內部函式定義
            def min_dist(point, current_df):
                return min([np.sqrt((point[0] - lat)**2 + (point[1] - lon)**2) for lat, lon in current_df[["latitude", "longitude"]].values])
            # 修改呼叫方式
            best_exit = max(candidates.items(), key=lambda x: min_dist(x[1], df_tight))
            
            # 確保有事故資料才計算最短距離 (安全護欄)
            if not df_tight.empty:
                best_exit = max(candidates.items(), key=lambda x: min_dist(x[1], df_tight))
                best_entry_name = best_exit[0]
                best_entry_coord = best_exit[1]
            else:
                best_entry_name = "無事故資料，皆可進入"
                best_entry_coord = [c_lat, c_lon] # 預設使用夜市中心點
            final_act3_guides[name] = {
                "peak_period": peak_period,
                "rain_increase": int(rain_ratio),
                "danger_zone": f"{danger_zone} ({zone_map[danger_zone]}件)",
                "best_entry_name": best_entry_name,
                "best_entry_coord": best_entry_coord}
            
    # 將行人步行建議導航推上Redis(使用 pickle)
    r.set("market:act3_guide_cache", pickle.dumps(final_act3_guides), ex=864000)
    print("行人步行建議導航已成功寫入 Redis！")


default_args = {
    'owner': 'traffic_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),}

with DAG('d_redis_nightmarket_data', default_args=default_args, schedule='0 4 1 * *', catchup=False) as dag:
    sync_task = PythonOperator(
        task_id='precompute_and_push_to_redis',
        python_callable=precompute_market_stats)