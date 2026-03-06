import pandas as pd
import numpy as np
from sqlalchemy import text
from c_db import get_db_engine
from r_cache import get_cache, set_cache, REDIS_POOL
import threading
import streamlit as st

# =========================================================
#  1. 夜市資料
# =========================================================
def get_all_nightmarkets():
    cache_key = "market:list_all_auto" 
    cached = get_cache(cache_key)
    if cached is not None:
        df_cached = pd.DataFrame(cached)
        if 'District' in df_cached.columns and 'City' in df_cached.columns:
            return df_cached
        else:
            print("快取資料缺少欄位(尚未更新)，正在自動更新...")

    engine = get_db_engine()
    if not engine: return pd.DataFrame()
    
    query = """
    SELECT nightmarket_name, nightmarket_latitude, nightmarket_longitude, 
           nightmarket_city, nightmarket_region, nightmarket_opening_hours 
    FROM `test_night_market`.`Night_market_merge`
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        if df.empty: return df

        df['lat'] = pd.to_numeric(df['nightmarket_latitude'], errors='coerce')
        df['lon'] = pd.to_numeric(df['nightmarket_longitude'], errors='coerce')
        df['MarketName'] = df['nightmarket_name']
        df['City'] = df['nightmarket_city']
        df['District'] = df['nightmarket_region'] 
        
        result = df.dropna(subset=['lat', 'lon'])
        set_cache(cache_key, result.to_dict('records'), ttl=86400)
        return result
    except Exception as e:
        print(f"夜市讀取失敗: {e}")
        return pd.DataFrame()

# =========================================================
#  2. 氣象測站
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

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda/2)**2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def find_nearest_station(target_lat, target_lon):
    df_stations = get_all_stations()
    if df_stations.empty: return None, 0
    distances = haversine_distance(target_lat, target_lon, df_stations['lat'].values, df_stations['lon'].values)
    min_idx = np.argmin(distances)
    return df_stations.iloc[min_idx].to_dict(), distances[min_idx]

# =========================================================
#  3. 交通熱力統計
# =========================================================
def get_taiwan_heatmap_data():
    cache_key = "traffic:global_heatmap_lite_v2"
    cached = get_cache(cache_key)
    if cached: return cached
    engine = get_db_engine()
    sql = "SELECT lat, lon, count FROM `test_accident`.`tbl_accident_heatmap` WHERE count >= 3"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
            if len(df) > 8000: df = df.sample(n=8000, random_state=42)
            result = df.values.tolist()
            set_cache(cache_key, result, ttl=86400)
            return result
    except: return []

    """
    回傳: 地圖點位, 統計數據, 圖表數據, 年份詳細統計
    sample: 是否進行資料抽樣 (預設 True 用於地圖顯示, False 用於分析)
    """

@st.cache_data(ttl=3600, show_spinner=False) 
def get_nearby_accidents(lat, lon, radius_km=0.5, sample=True):
    strict_key = f"traffic:nearby_v12:{lat:.4f}_{lon:.4f}_3.0_all_sample"
    cached = get_cache(strict_key)
    if not cached:
        airflow_key = f"traffic:nearby_v12:{round(lat,4)}_{round(lon,4)}_3.0_all_sample"
        if strict_key != airflow_key:
            print(f"比對未命中，嘗試讀取 Airflow 相容金鑰: {airflow_key} ...")
            cached = get_cache(airflow_key)

    # ==========================================
    # 成功攔截快取後的處理
    # ==========================================
    if cached:
        df_all, _, _, _ = cached 
        distances = haversine_distance(lat, lon, df_all['latitude'].values, df_all['longitude'].values)
        df_filtered = df_all[distances <= radius_km]
        print(f"快取讀取成功！提取 {radius_km}km 範圍")
        return (df_filtered, {"total": len(df_filtered)}, {}, pd.DataFrame())

    # ==========================================
    # 🚨 階段 3：徹底無快取，去 MySQL 撈取 final 表 (附帶背景預熱)
    # ==========================================
    print(f"❌無快取，啟動DB 查詢 {radius_km}km 範圍...")
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
        FROM `test_accident`.`tbl_accident_analysis_final`
        WHERE latitude BETWEEN :min_lat AND :max_lat 
          AND longitude BETWEEN :min_lon AND :max_lon
    """)

    max_offset_fast = radius_km / 111.0
    params_fast = {"min_lat": lat-max_offset_fast, "max_lat": lat+max_offset_fast, "min_lon": lon-max_offset_fast, "max_lon": lon+max_offset_fast}
    
    try:
        with engine.connect() as conn:
            df_fast = pd.read_sql(sql, conn, params=params_fast)
    except Exception as e:
        return pd.DataFrame(), {"total":0}, {}, pd.DataFrame()

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
    FROM `test_accident`.`tbl_pedestrian_accident`
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
    FROM `test_accident`.`tbl_pedestrian_accident`
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
    """
    分析該夜市在不同天氣下的事故佔比與死傷情形
    """
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
        FROM `test_accident`.`tbl_accident_analysis_final`
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