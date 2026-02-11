import pandas as pd
import numpy as np
from sqlalchemy import text
from c_db import get_db_engine
from r_cache import get_cache, set_cache

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
    sql = "SELECT lat, lon, count FROM `test_accident`.`view_accident_heatmap` WHERE count >= 3"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
            if len(df) > 8000: df = df.sample(n=8000, random_state=42)
            result = df.values.tolist()
            set_cache(cache_key, result, ttl=86400)
            return result
    except: return []

def get_nearby_accidents(lat, lon, radius_km=0.5, sample=True):
    """
    回傳: 地圖點位, 統計數據, 圖表數據, 年份詳細統計
    sample: 是否進行資料抽樣 (預設 True 用於地圖顯示, False 用於分析)
    """
    sample_tag = "sample" if sample else "full"
    cache_key = f"traffic:nearby_v10:{round(lat,4)}_{round(lon,4)}_{radius_km}_{sample_tag}"
    cached = get_cache(cache_key)
    if cached: return cached

    engine = get_db_engine()
    offset = float(radius_km) / 111.0
    sql = text("""
        SELECT 
            m.accident_datetime,
            m.latitude, 
            m.longitude, 
            m.death_count, 
            m.injury_count, 
            m.weather_condition,
            MAX(COALESCE(p.cause_analysis_minor_primary, p.cause_analysis_major_primary, '未知')) AS primary_cause
        FROM `test_accident`.`accident_sq1_main` m
        LEFT JOIN `test_accident`.`accident_sq1_process` p 
            ON m.accident_id = p.accident_id
        WHERE m.latitude BETWEEN :min_lat AND :max_lat
          AND m.longitude BETWEEN :min_lon AND :max_lon
        GROUP BY m.accident_id
    """)
    params = {"min_lat": lat-offset, "max_lat": lat+offset, "min_lon": lon-offset, "max_lon": lon+offset}

    df = pd.DataFrame()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
    except Exception as e:
        print(f"查詢失敗: {e}")
        return pd.DataFrame(), {"total":0}, {}, pd.DataFrame()
    
    if df.empty: 
        return pd.DataFrame(), {"total":0}, {}, pd.DataFrame()

    df['accident_datetime'] = pd.to_datetime(df['accident_datetime'], errors='coerce')
    df['Year'] = df['accident_datetime'].dt.year
    df['Hour'] = df['accident_datetime'].dt.hour
    df['Weekday'] = df['accident_datetime'].dt.day_name()
    
    def get_period(h):
        if 6 <= h < 12: return '早'
        elif 12 <= h < 18: return '午'
        else: return '晚'
    df['Period'] = df['Hour'].apply(get_period)
    
    wd_map = {'Monday':'週一', 'Tuesday':'週二', 'Wednesday':'週三', 'Thursday':'週四', 'Friday':'週五', 'Saturday':'週六', 'Sunday':'週日'}
    df['Weekday_CN'] = df['Weekday'].map(wd_map)

    stats = {
        "total": len(df),
        "dead": int(df['death_count'].sum()),
        "hurt": int(df['injury_count'].sum())}
    
    top10 = df['primary_cause'].value_counts().head(10).reset_index()
    top10.columns = ['肇因', '件數']
    year_trend = df.groupby('Year').size().reset_index(name='件數')
    week_stats = df.groupby('Weekday_CN').size().reindex(['週一','週二','週三','週四','週五','週六','週日'], fill_value=0).reset_index(name='件數')
    hour_stats = df.groupby('Hour').size().reset_index(name='件數')
    
    if 'weather_condition' in df.columns:
        weather_stats = df['weather_condition'].value_counts().reset_index()
        weather_stats.columns = ['天氣', '件數']
    else:
        weather_stats = pd.DataFrame(columns=['天氣', '件數'])

    charts = {'top10': top10, 'year': year_trend, 'week': week_stats, 'hour': hour_stats, 'weather': weather_stats}

    yearly_grp = df.groupby('Year').agg({'death_count': 'sum', 'injury_count': 'sum', 'accident_datetime': 'count'}).rename(columns={'accident_datetime': 'Total', 'death_count':'Dead', 'injury_count':'Hurt'})
    period_grp = pd.crosstab(df['Year'], df['Period'])
    for p in ['早', '午', '晚']:
        if p not in period_grp.columns: period_grp[p] = 0
    yearly_stats = pd.concat([yearly_grp, period_grp], axis=1).sort_index(ascending=False).reset_index()

    if sample:
        map_df = df.sample(n=500, random_state=42) if len(df) > 500 else df
    else:
        map_df = df
    result = (map_df, stats, charts, yearly_stats)
    set_cache(cache_key, result, ttl=3600)
    return result

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
    FROM `test_accident`.`view_pedestrian_accident`
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
        FROM `test_accident`.`accident_sq1_main`
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