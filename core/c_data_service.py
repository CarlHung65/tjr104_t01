import pandas as pd
import numpy as np
from sqlalchemy import text
from .c_db import get_db_engine
from .r_cache import get_cache, set_cache, REDIS_POOL
import threading
import streamlit as st
import datetime
import random 
import re

# =========================================================
#  夜市資料
# =========================================================
# 讀取全台夜市主檔並清洗經緯度
def get_all_nightmarkets():
    cache_key = "market:list_all_auto_v3" 
    cached = get_cache(cache_key)
    if cached is not None:
        df_cached = pd.DataFrame(cached)
        if 'AdminDistrict' in df_cached.columns and 'Region' in df_cached.columns:
            return df_cached

    engine = get_db_engine()
    if not engine: return pd.DataFrame()
    
    query = """
    SELECT * FROM `test_night_market`.`Night_market_merge`
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        if df.empty: return df

        # 資料清洗：確保經緯度為數值型別，並補上四層級分類標籤供前端下拉選單使用
        df['lat'] = pd.to_numeric(df['nightmarket_latitude'], errors='coerce')
        df['lon'] = pd.to_numeric(df['nightmarket_longitude'], errors='coerce')
        df['MarketName'] = df['nightmarket_name']
        
        # 綁定四層級：
        df['Region'] = df['nightmarket_region']             # 第一層：北部
        df['City'] = df['nightmarket_city']                 # 第二層：臺北市
        df['AdminDistrict'] = df['nightmarket_zipcode_name']# 第三層：中正區/大同區
        
        # 向後相容舊程式碼
        df['District'] = df['Region'] 
        
        result = df.dropna(subset=['lat', 'lon']) # 剔除經緯度遺漏的髒資料
        set_cache(cache_key, result.to_dict('records'), ttl=86400)
        return result
    except Exception as e:
        print(f"夜市讀取失敗: {e}")
        return pd.DataFrame()

# =========================================================
#  氣象測站
# =========================================================
def get_all_stations():
    cache_key = "weather:all_stations"
    cached = get_cache(cache_key)
    if cached is not None: return pd.DataFrame(cached)
    engine = get_db_engine()
    sql = "SELECT station_id, station_name, station_latitude_WGS84 as lat, station_longitude_WGS84 as lon FROM `test_weather`.`obs_stations`"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
        set_cache(cache_key, df.to_dict('records'), ttl=86400)
        return df
    except: return pd.DataFrame()

# 計算邏輯：Haversine 半正矢公式
# 目的：用來計算地球表面兩點之間的大圓距離
# 使用 numpy 進行向量化運算，比寫 for 迴圈逐筆算快上幾百倍。 R=6371 為地球半徑(km)
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda/2)**2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

# 尋找離指定座標最近的氣象測站
def find_nearest_station(target_lat, target_lon):
    df_stations = get_all_stations()
    if df_stations.empty: return None, 0
    distances = haversine_distance(target_lat, target_lon, df_stations['lat'].values, df_stations['lon'].values)
    min_idx = np.argmin(distances) # 取出距離陣列中最小值所在的索引
    return df_stations.iloc[min_idx].to_dict(), distances[min_idx]

# =========================================================
#  交通熱力統計
# =========================================================
def get_taiwan_heatmap_data():
    cache_key = "traffic:global_heatmap_lite_v2"
    cached = get_cache(cache_key)
    if cached: return cached
    engine = get_db_engine()
    # 僅撈取同一座標重複發生 3 次以上的「熱點」，大幅縮小前端記憶體消耗
    sql = "SELECT lat, lon, count FROM frontend_db_consol.tbl_accident_heatmap WHERE count >= 3"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
            # [教學說明] 效能保護：如果熱力圖資料點超過 8000 點，強制隨機抽樣，避免地圖卡頓
            if len(df) > 8000: df = df.sample(n=8000, random_state=42)
            result = df.values.tolist()
            set_cache(cache_key, result, ttl=86400)
            return result
    except: return []

# 核心事故撈取引擎 (含背景預熱機制)
# 使用 st.cache_data 進行 Streamlit 的本地記憶體快取
@st.cache_data(ttl=3600, show_spinner=False) 
def get_nearby_accidents(lat, lon, radius_km=0.5, sample=True):
    strict_key = f"traffic:nearby_v12:{lat:.4f}_{lon:.4f}_3.0_all_sample" # Streamlit 最新標準寫法
    cached = get_cache(strict_key)
    if not cached:
        airflow_key = f"traffic:nearby_v12:{round(lat,4)}_{round(lon,4)}_3.0_all_sample" # 相容 Airflow 的舊寫
        if strict_key != airflow_key:
            print(f"⚠️ 嚴格比對未命中，嘗試讀取 Airflow 相容金鑰: {airflow_key} ...")
            cached = get_cache(airflow_key)
    if cached:
        df_all, _, _, _ = cached 

        # 如果 Redis 中已經有，就直接用 Haversine 裁切出使用者要的半徑範圍
        distances = haversine_distance(lat, lon, df_all['latitude'].values, df_all['longitude'].values)
        df_filtered = df_all[distances <= radius_km]
        print(f"✅ 快取讀取成功！提取 {radius_km}km 範圍")
        return (df_filtered, {"total": len(df_filtered)}, {}, pd.DataFrame())

    # 無快取，去 MySQL 撈取 final 表 (附帶背景預熱)
    print(f"❌ 徹無快取，啟動DB 查詢 {radius_km}km 範圍...")
    engine = get_db_engine()
    
    sql = text("""
        SELECT accident_datetime, latitude, longitude, death_count, 
               injury_count, weather_condition, primary_cause,
               Year, Hour, Weekday_CN,
               CASE 
                   WHEN Hour >= 6 AND Hour < 12 THEN '早'
                   WHEN Hour >= 12 AND Hour < 18 THEN '午'
                   ELSE '晚'
               END AS Period
        FROM frontend_db_consol.`tbl_accident_analysis_final`
        WHERE latitude BETWEEN :min_lat AND :max_lat 
          AND longitude BETWEEN :min_lon AND :max_lon
    """)

    # 計算邏輯：經緯度快速過濾法
    # 1 度經緯度約等於 111 公里。用正方形邊界先請 DB 快速篩選，再進 Python 用 Haversine 算圓形半徑
    max_offset_fast = radius_km / 111.0
    params_fast = {"min_lat": lat-max_offset_fast, "max_lat": lat+max_offset_fast, "min_lon": lon-max_offset_fast, "max_lon": lon+max_offset_fast}
    try:
        with engine.connect() as conn:
            df_fast = pd.read_sql(sql, conn, params=params_fast)
    except Exception as e:
        return pd.DataFrame(), {"total":0}, {}, pd.DataFrame()

    # 多執行緒背景預熱
    # 為提升使用者體驗，先吐出使用者要的 0.5km 資料，同時開一個背景執行緒偷偷把其他範圍結果存進 Redis
    # 下次使用者拉大範圍時，就能瞬間從 Redis 讀取
    def background_warmup():
        print(f"背景任務啟動：正在為此夜市撈取 3.0km Master Bag...")
        try:
            max_offset_bg = 3.0 / 111.0
            params_bg = {"min_lat": lat-max_offset_bg, "max_lat": lat+max_offset_bg, "min_lon": lon-max_offset_bg, "max_lon": lon+max_offset_bg}
            with engine.connect() as conn_bg:
                df_bg = pd.read_sql(sql, conn_bg, params=params_bg)
            if not df_bg.empty:
                set_cache(strict_key, (df_bg, {"total": len(df_bg)}, {}, pd.DataFrame()), ttl=172800)
                print("背景任務完成！")
        except Exception as e:
            pass

    bg_thread = threading.Thread(target=background_warmup)
    bg_thread.start()

    if df_fast.empty: return pd.DataFrame(), {"total":0}, {}, pd.DataFrame()
    # 對 DB 提供的正方形範圍進行圓形過濾
    distances = haversine_distance(lat, lon, df_fast['latitude'].values, df_fast['longitude'].values)
    df_filtered = df_fast[distances <= radius_km]
    return (df_filtered, {"total": len(df_filtered)}, {}, pd.DataFrame())

def get_pedestrian_stats_by_region_monthly():
    cache_key = "analysis:pedestrian_region_month" 
    cached = get_cache(cache_key)
    if cached: return pd.DataFrame(cached)

    engine = get_db_engine()
    sql = """
    SELECT 
        DATE_FORMAT(accident_datetime, '%%Y-%%m') as YearMonth,
        CASE 
            WHEN longitude > 121.5 AND latitude < 24.5 THEN '東部' 
            WHEN latitude > 24.45 THEN '北部'
            WHEN latitude > 23.45 THEN '中部'
            ELSE '南部'
        END as Region,
        COUNT(DISTINCT accident_id) as Count
    FROM `frontend_db_consol`.`tbl_pedestrian_accident`
    GROUP BY YearMonth, Region
    ORDER BY YearMonth, Region
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
        set_cache(cache_key, df.to_dict('records'), ttl=86400)
        return df
    except Exception as e:
        return pd.DataFrame()

def get_pedestrian_trend(lat=None, lon=None, radius_km=0.5):
    if lat is None or lon is None:
        cache_key = "analysis:pedestrian_trend_global_v2"
        where_clause = ""
        params = {}
    else:
        cache_key = f"analysis:pedestrian_trend_local_v2:{round(lat,4)}_{round(lon,4)}"
        offset = float(radius_km) / 111.0
        where_clause = """
            WHERE latitude BETWEEN :min_lat AND :max_lat
              AND longitude BETWEEN :min_lon AND :max_lon
        """
        params = {
            "min_lat": lat - offset, "max_lat": lat + offset, 
            "min_lon": lon - offset, "max_lon": lon + offset
        }

    cached = get_cache(cache_key)
    if cached: return pd.DataFrame(cached)

    engine = get_db_engine()
    sql = text(f"""
    SELECT 
        DATE_FORMAT(accident_datetime, '%Y-%m') as YearMonth,
        COUNT(DISTINCT accident_id) as Count
    FROM `frontend_db_consol`.`tbl_pedestrian_accident`
    {where_clause}
    GROUP BY YearMonth
    ORDER BY YearMonth
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
        set_cache(cache_key, df.to_dict('records'), ttl=86400)
        return df
    except Exception as e:
        return pd.DataFrame()

# 天候風險分析 (包含死傷嚴重度)
def get_accident_weather_analysis(lat, lon, radius_km=0.5):
    cache_key = f"analysis:weather_risk:{round(lat,4)}_{round(lon,4)}_{radius_km}"
    cached = get_cache(cache_key)
    if cached: return pd.DataFrame(cached)

    engine = get_db_engine()
    offset = float(radius_km) / 111.0
    
    sql = text("""
        SELECT 
            weather_condition as 天氣, 
            COUNT(*) as 件數,
            SUM(death_count) as 死亡,
            SUM(injury_count) as 受傷
        FROM `frontend_db_consol`.`tbl_accident_analysis_final`
        WHERE latitude BETWEEN :min_lat AND :max_lat 
          AND longitude BETWEEN :min_lon AND :max_lon
        GROUP BY weather_condition
        ORDER BY 件數 DESC
    """)
    params = {"min_lat": lat-offset, "max_lat": lat+offset, "min_lon": lon-offset, "max_lon": lon+offset}
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
        set_cache(cache_key, df.to_dict('records'), ttl=3600)
        return df
    except Exception as e:
        print(f"天氣分析失敗: {e}")
        return pd.DataFrame()

# 危險等級判別 (⭐ 原本 market_tools 內的 danger_level 功能與此重複，統一使用此函式)
def get_dynamic_level(val, avg_val):
    if avg_val == 0: return "🟢 安全"
    if val > avg_val * 1.3: return "🔴 極危險"
    elif val > avg_val * 1.2: return "🟠 危險"
    elif val > avg_val * 1.1: return "🟡 注意"
    else: return "🟢 安全"

# 排名計算邏輯
def calculate_rank_changes(df, entity_col, metric_col, top_n=5, ascending=False, time_col='Year'):
    if df.empty: return pd.DataFrame()
    
    # 判斷是計算總數還是加總分數
    if metric_col == 'accident_id':
        yearly_df = df.groupby([time_col, entity_col])[metric_col].count().reset_index()
    else:
        yearly_df = df.groupby([time_col, entity_col])[metric_col].sum().reset_index()
        
    yearly_df = yearly_df.rename(columns={metric_col: 'metric_val'})
    yearly_df['Rank'] = yearly_df.groupby(time_col)['metric_val'].rank(ascending=ascending, method='first')
    
    rank_pivot = yearly_df.pivot(index=entity_col, columns=time_col, values='Rank')
    years = sorted(yearly_df[time_col].unique(), reverse=True)
    display_years = [y for y in years if int(y) >= 2023]
    
    result_dict = {}
    for y in display_years:
        prev_y = type(y)(int(y) - 1) 
        current_top = yearly_df[(yearly_df[time_col] == y) & (yearly_df['Rank'] <= top_n)].sort_values('Rank')
        
        formatted_list = []
        for _, row in current_top.iterrows():
            entity = row[entity_col]
            curr_rank = row['Rank']
            
            if prev_y in rank_pivot.columns and not pd.isna(rank_pivot.at[entity, prev_y]):
                prev_rank = rank_pivot.at[entity, prev_y]
                diff = prev_rank - curr_rank
                if diff > 0: trend = f"🔼 {int(diff)}"
                elif diff < 0: trend = f"🔽 {int(-diff)}"
                else: trend = "➖"
            else:
                trend = "🆕"
            formatted_list.append(f"{entity} ({trend})")
            
        while len(formatted_list) < top_n:
            formatted_list.append("-")
        result_dict[str(y)] = formatted_list
        
    res_df = pd.DataFrame(result_dict)
    if not res_df.empty:
        res_df.insert(0, '名次', [f"第 {i} 名" for i in range(1, top_n + 1)])
    return res_df

# =========================================================
# 新增組員資料 - market_tools.py 的地理與時間運算邏輯
# =========================================================
# 解析資料庫中的營業時間 JSON 格式
def parse_opening_hours(opening_str):
    segments = opening_str.split("/")
    result = {}
    for seg in segments:
        seg = seg.strip()
        if not seg: continue
        # 使用正則表達式擷取
        weekday_match = re.search(r'weekday"\s*:\s*(\d+)', seg)
        open_match = re.search(r'open"\s*:\s*"(\d{2}:\d{2})"', seg)
        close_match = re.search(r'close"\s*:\s*"(\d{2}:\d{2})"', seg)
        if weekday_match and open_match and close_match:
            weekday = int(weekday_match.group(1))
            result[weekday] = (open_match.group(1), close_match.group(1))
    return result

# 判斷事故是否發生在「夜市營業期間」
def is_in_opening(acc_time, opening_hours):
    weekday = acc_time.weekday()
    if weekday not in opening_hours: return False
    start_str, end_str = opening_hours[weekday]
    start = datetime.datetime.strptime(start_str, "%H:%M").time()
    end = datetime.datetime.strptime(end_str, "%H:%M").time()
    if start <= end:
        return start <= acc_time.time() <= end
    else: # 處理跨日營業的情況 (例如 17:00 ~ 02:00)
        return acc_time.time() >= start or acc_time.time() <= end

# 建立夜市方框，用於碰撞偵測
def get_bbox(nm):
    return {
        "ne_lat": float(nm["nightmarket_northeast_latitude"]),
        "ne_lon": float(nm["nightmarket_northeast_longitude"]),
        "sw_lat": float(nm["nightmarket_southwest_latitude"]),
        "sw_lon": float(nm["nightmarket_southwest_longitude"]),
    }

def accident_in_bbox(lat, lon, bbox):
    return (bbox["sw_lat"] <= lat <= bbox["ne_lat"] and bbox["sw_lon"] <= lon <= bbox["ne_lon"])

# 計算方式：PDI (Pedestrian Danger Index) 危險指數
# 將事故嚴重程度量化。營業期間加權=1.5，死亡=10分，受傷=2分
def calculate_pdi(acc_df, nm_df):
    WEIGHT_DEATH, WEIGHT_INJURY, WEIGHT_OPEN, WEIGHT_CLOSE = 10, 2, 1.5, 1
    results = []
    for _, nm in nm_df.iterrows():
        bbox = get_bbox(nm)
        opening_hours = parse_opening_hours(nm["nightmarket_opening_hours"])
        acc_in_nm = acc_df[acc_df.apply(lambda row: accident_in_bbox(row["latitude"], row["longitude"], bbox), axis=1)]
        total_pdi = 0
        for _, acc in acc_in_nm.iterrows():
            acc_time = acc["accident_datetime"]
            severity = acc["death_count"] * WEIGHT_DEATH + acc["injury_count"] * WEIGHT_INJURY
            weight = WEIGHT_OPEN if is_in_opening(acc_time, opening_hours) else WEIGHT_CLOSE
            total_pdi += severity * weight
        results.append({"nightmarket_name": nm["nightmarket_name"], "accident_count": len(acc_in_nm), "pdi": total_pdi})
    return pd.DataFrame(results)

# =========================================================
# 新增組員資料 -  market_tools.py 的洞察文字生成器
# 仿造 LLM 的文字生成器
# 利用 random.choice 建立隨機模板庫
# =========================================================
def generate_insight_V1(acc_df, market_df, selected_market):
    nm_row = market_df[market_df["nightmarket_name"] == selected_market]
    if nm_row.empty: return "找不到夜市資料。"
    nm = nm_row.iloc[0]
    nm_lat, nm_lon = float(nm["nightmarket_latitude"]), float(nm["nightmarket_longitude"])
    
    # 計算歐式距離過濾周邊事故
    acc_df["distance"] = ((acc_df["latitude"].astype(float) - nm_lat)**2 + (acc_df["longitude"].astype(float) - nm_lon)**2)**0.5
    nearby = acc_df[acc_df["distance"] < 0.005]
    if nearby.empty: return f"{selected_market} 周邊目前沒有事故資料。"

    total = len(nearby)
    deaths = nearby["death_count"].sum()
    injuries = nearby["injury_count"].sum()

    nearby["hour"] = pd.to_datetime(nearby["accident_datetime"]).dt.hour
    peak_hour = nearby["hour"].mode()[0] # 找出發生頻率最高的小時(眾數)

    if 5 <= peak_hour < 12: time_label = "早上"
    elif 12 <= peak_hour < 17: time_label = "下午"
    elif 17 <= peak_hour < 22: time_label = "晚上"
    else: time_label = "深夜"

    # 動態變數嵌入字串模板
    accident_templates = [
        f"<u>{selected_market}</u>  周邊共有 {total} 件事故，造成 {deaths} 人死亡、{injuries} 人受傷。",
        f"<u>{selected_market}</u>  周邊發生 {total} 件事故，死傷共 {deaths + injuries} 人。",
        f"<u>{selected_market}</u>  附近近期累計 {total} 件事故，造成 {deaths} 死 {injuries} 傷。",
        f"<u>{selected_market}</u>  周邊事故量為 {total} 件，死傷情況為 {deaths} 死亡、{injuries} 受傷。"]

    time_templates = [
        f"<u>{selected_market}</u>  事故最常發生在 {time_label}（約 {peak_hour} 時）。",
        f"<u>{selected_market}</u>  事故高峰落在 {time_label}（約 {peak_hour} 時）。",
        f"<u>{selected_market}</u>  最容易發生事故的時段是 {time_label}（約 {peak_hour} 時）。",
        f"<u>{selected_market}</u>  事故多集中在 {time_label}（約 {peak_hour} 時）。"]

    return random.choice(random.choice([accident_templates, time_templates]))

def generate_insight_V2(acc_df, market_df, selected_market, human_df=None, road_df=None):
    nm_row = market_df[market_df["nightmarket_name"] == selected_market]
    if nm_row.empty: return "找不到夜市資料。"
    nm = nm_row.iloc[0]
    nm_lat, nm_lon = float(nm["nightmarket_latitude"]), float(nm["nightmarket_longitude"])

    acc_df = acc_df.copy()
    acc_df["distance"] = ((acc_df["latitude"].astype(float) - nm_lat)**2 + (acc_df["longitude"].astype(float) - nm_lon)**2)**0.5
    nearby = acc_df[acc_df["distance"] < 0.005]
    if nearby.empty: return f"{selected_market} 周邊目前沒有事故資料。"

    df = nearby.copy()
    if human_df is not None: df = df.merge(human_df, on="accident_id", how="left")
    if road_df is not None: df = df.merge(road_df, on="accident_id", how="left")

    total, deaths, injuries = len(df), df["death_count"].sum(), df["injury_count"].sum()
    severity = deaths * 5 + injuries * 2

    df["hour"] = pd.to_datetime(df["accident_datetime"], errors="coerce").dt.hour
    peak_hour = int(df["hour"].mode()[0]) if df["hour"].notna().any() else None

    if peak_hour is None: time_label = "不明時段"
    elif 5 <= peak_hour < 12: time_label = "早上"
    elif 12 <= peak_hour < 17: time_label = "下午"
    elif 17 <= peak_hour < 22: time_label = "晚上"
    else: time_label = "深夜"

    top_cause = df["cause_analysis_minor_primary"].mode()[0] if "cause_analysis_minor_primary" in df and df["cause_analysis_minor_primary"].notna().any() else "無事故原因資料"
    top_violation = df["party_action_minor"].mode()[0] if "party_action_minor" in df and df["party_action_minor"].notna().any() else "無違規資料"

    templates = [
        f"<u>{selected_market}</u> 周邊 500 公尺內共發生 {total} 件事故，死傷 {deaths + injuries} 人，事故高峰多出現在 {time_label}（約 {peak_hour} 時）。",
        f"<u>{selected_market}</u> 近期事故量為 {total} 件，主要事故原因為「{top_cause}」，最常見違規行為為「{top_violation}」。",
        f"<u>{selected_market}</u> 周邊事故造成 {deaths} 死亡、{injuries} 受傷，事故多集中在 {time_label}（{peak_hour} 時）。",
        f"<u>{selected_market}</u> 夜市附近事故以「{top_cause}」最常見，並在 {time_label} 時段達到高峰。",
        f"<u>{selected_market}</u> 事故嚴重度評估為 {severity} 分，主要違規行為為「{top_violation}」。"]
    return random.choice(templates)

def generate_insight_V3(selected_market, grid_df, best_exit):
    def cell_to_direction(row, col, grid_size=3):
        center = grid_size // 2
        if row < center and col < center: return "西北"
        if row < center and col > center: return "東北"
        if row > center and col < center: return "西南"
        if row > center and col > center: return "東南"
        if row == center and col < center: return "正西"
        if row == center and col > center: return "正東"
        if col == center and row < center: return "正北"
        if col == center and row > center: return "正南"
        return "中心"

    best_exit_name = best_exit[0] if isinstance(best_exit, (list, tuple)) else best_exit
    if grid_df.empty: return f"{selected_market} 周邊目前沒有事故資料。"

    high_risk_cells = grid_df[grid_df["color"] == "red"]
    low_risk_cells = grid_df[grid_df["color"] == "green"]

    if not high_risk_cells.empty:
        worst_cell = high_risk_cells.sort_values("score", ascending=False).iloc[0]
        worst_score, worst_count = int(worst_cell["score"]), int(worst_cell["accident_count"])
        worst_dir = cell_to_direction(worst_cell["grid_row"], worst_cell["grid_col"])
    else:
        worst_dir, worst_score, worst_count = "無", 0, 0

    if not low_risk_cells.empty:
        safest_cell = low_risk_cells.sort_values("score", ascending=True).iloc[0]
        safest_dir = cell_to_direction(safest_cell["grid_row"], safest_cell["grid_col"])
    else: safest_dir = "無"

    templates = [
        f"{selected_market}最危險的區段位於 {worst_dir}側，累積風險分數 {worst_score}，共發生 {worst_count} 件事故。",
        f"{selected_market}最安全的區段位於 {safest_dir}側，適合作為行人通行或避開事故熱點的路線。",
        f"建議從 {best_exit_name} 進入{selected_market}，該入口距離事故點最遠，安全性最高。",
        f"整體來看，{selected_market}最高風險集中在 {worst_dir}側，最安全區段則位於 {safest_dir}側，從 {best_exit_name} 進入能有效降低風險。"]
    return random.choice(templates)

def generate_insight_V4(city_rank):
    if city_rank.empty: return "目前沒有足夠的資料產生城市排行榜洞察。"
    sorted_rank = city_rank.sort_values("平均PDI", ascending=False)
    
    top_city = sorted_rank.iloc[0]
    top_name, top_pdi, top_level, top_acc, top_nm = top_city["城市"], top_city["平均PDI"], top_city["危險等級"], top_city["事故總數"], top_city["夜市數量"]
    
    safe_city = sorted_rank.iloc[-1]
    safe_name, safe_pdi, safe_level, safe_acc, safe_nm = safe_city["城市"], safe_city["平均PDI"], safe_city["危險等級"], safe_city["事故總數"], safe_city["夜市數量"]
    
    avg_pdi = round(city_rank["平均PDI"].mean(), 1)

    templates = [
        f"{top_name} 的夜市平均 PDI 達到 {top_pdi}（{top_level}），是目前最需要改善行人安全的城市之一，共有 {top_nm} 個夜市、累積 {top_acc} 件事故。",
        f"{safe_name} 的夜市平均 PDI 僅 {safe_pdi}（{safe_level}），在所有城市中相對安全，事故量 {safe_acc} 件、夜市數量 {safe_nm} 個。",
        f"{top_name} 與 {safe_name} 的夜市安全差距明顯，前者平均 PDI 高達 {top_pdi}，後者僅 {safe_pdi}，顯示不同城市之間的夜市風險差異巨大。",
        f"全台夜市城市的平均 PDI 約為 {avg_pdi}，其中 {top_name} 風險最高，而 {safe_name} 相對最安全。",
        f"整體來看，{top_name} 的夜市風險集中且事故量高，而 {safe_name} 的夜市相對安全，城市之間的夜市安全程度呈現明顯落差。"]
    return random.choice(templates)