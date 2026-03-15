from folium.plugins import HeatMap
from streamlit_folium import st_folium
from folium.features import DivIcon
import random
import folium
import streamlit as st
import pandas as pd
import requests
import polyline
import core.c_data_service as ds
import core.c_ui as ui
import redis
import pickle
from core.r_cache import REDIS_POOL

# 1. 取得夜市基礎資料
# 呼叫共用服務取得全台夜市清單
nm_df = ds.get_all_nightmarkets()

# 確保必要欄位存在 (相容舊版資料表命名)
if 'nightmarket_name' not in nm_df.columns: nm_df['nightmarket_name'] = nm_df['MarketName']
if 'nightmarket_city' not in nm_df.columns: nm_df['nightmarket_city'] = nm_df['City']
if 'nightmarket_latitude' not in nm_df.columns: nm_df['nightmarket_latitude'] = nm_df['lat']
if 'nightmarket_longitude' not in nm_df.columns: nm_df['nightmarket_longitude'] = nm_df['lon']

# 統一城市名稱格式，避免比對錯誤
nm_df['nightmarket_city'] = nm_df['nightmarket_city'].str.replace('台', '臺')

# 2. 區域與行政區正規化
# 如果資料庫沒有提供 Region (北中南東)，則利用字典對應產生
if 'District' in nm_df.columns and nm_df['District'].isin(['北部', '中部', '南部', '東部', '離島']).any():
    nm_df['Region'] = nm_df['District'] 
else:
    region_map = {
        "臺北市": "北部", "新北市": "北部", "基隆市": "北部", "桃園市": "北部", "新竹市": "北部", "新竹縣": "北部", "宜蘭縣": "北部",
        "臺中市": "中部", "苗栗縣": "中部", "彰化縣": "中部", "南投縣": "中部", "雲林縣": "中部",
        "臺南市": "南部", "高雄市": "南部", "嘉義市": "南部", "嘉義縣": "南部", "屏東縣": "南部",
        "臺東縣": "東部", "花蓮縣": "東部",
        "澎湖縣": "離島", "金門縣": "離島", "連江縣": "離島"
    }
    nm_df['Region'] = nm_df['nightmarket_city'].map(region_map).fillna("其他")

# 尋找可用的行政區欄位，並統一命名為 AdminDistrict
possible_admin_cols = ['Town', 'Township', 'Area', '鄉鎮市區', 'admin_district', 'AdminDistrict']
admin_col_found = False
for col in possible_admin_cols:
    if col in nm_df.columns and not nm_df[col].isin(['北部', '中部', '南部', '東部']).any():
        nm_df['AdminDistrict'] = nm_df[col]
        admin_col_found = True
        break
if not admin_col_found:
    nm_df['AdminDistrict'] = nm_df['District'] if 'District' in nm_df.columns and not nm_df['District'].isin(['北部', '中部', '南部', '東部']).any() else "全區"

# 主渲染函式
def act3_render():
    # 清除跨頁面殘留的 session 狀態
    if "page_data" in st.session_state:
        st.session_state["page_data"].clear()

    # 3. UI 選擇器設計
    # 提供兩種搜尋模式：層層篩選 vs 關鍵字直搜
    search_mode = st.radio("尋找方式：", ["🗺️ 區域層層篩選", "🔍 直接關鍵字搜尋"], horizontal=True)
    def_region, def_city, def_dist, def_market = "北部", "臺北市", "士林區", "士林夜市"
    
    if search_mode == "🗺️ 區域層層篩選":
        # 建立四個連動的下拉選單
        col_r, col_c, col_d, col_m = st.columns(4)
        with col_r:
            regions = sorted(nm_df["Region"].unique())
            selected_region = st.selectbox("選擇區域", regions, index=regions.index(def_region) if def_region in regions else 0)
        with col_c:
            cities = sorted(nm_df[nm_df["Region"] == selected_region]["nightmarket_city"].unique())
            selected_city = st.selectbox("選擇縣市", cities, index=cities.index(def_city) if (selected_region == def_region and def_city in cities) else 0)
        with col_d:
            dists = sorted(nm_df[(nm_df["Region"] == selected_region) & (nm_df["nightmarket_city"] == selected_city)]["AdminDistrict"].unique())
            selected_dist = st.selectbox("選擇行政區", dists, index=dists.index(def_dist) if (selected_city == def_city and def_dist in dists) else 0)
        with col_m:
            markets = sorted(nm_df[(nm_df["Region"] == selected_region) & (nm_df["nightmarket_city"] == selected_city) & (nm_df["AdminDistrict"] == selected_dist)]["nightmarket_name"].unique())
            if not markets:
                st.warning("此區域無夜市")
                return
            selected_market = st.selectbox("選擇夜市", markets, index=markets.index(def_market) if (selected_dist == def_dist and def_market in markets) else 0)
    else:
        # 直接列出所有夜市供搜尋
        all_markets = sorted(nm_df["nightmarket_name"].unique())
        selected_market = st.selectbox("請輸入或選擇夜市名稱：", all_markets, index=all_markets.index(def_market) if def_market in all_markets else 0)

    # 取得被選中夜市的詳細屬性
    nm_match = nm_df[nm_df["nightmarket_name"] == selected_market]
    if nm_match.empty:
        st.error(f"找不到夜市：{selected_market}，請重新選擇。")
        return
    nm_row = nm_match.iloc[0]

    # 4. Redis 快取讀取 (效能核心)
    try:
        r = redis.Redis(connection_pool=REDIS_POOL)
        
        # 讀取一：從 Airflow 排程算好的懶人包 (包含最佳出口、危險區域等)，省去前端複雜運算
        guide_cache = r.get("market:act3_guide_cache")
        guide_data = pickle.loads(guide_cache) if guide_cache else {}
        market_guide = guide_data.get(selected_market)
        
        # 讀取二：抓取該夜市周邊事故原始點位，專供地圖繪製熱力圖使用
        cache_key = f"traffic:nearby_v12:{nm_row['lat']:.4f}_{nm_row['lon']:.4f}_3.0_all_sample"
        raw_data = r.get(cache_key)
        date_filtered_df = pd.DataFrame()
        if raw_data:
            result = pickle.loads(raw_data)
            date_filtered_df = result[0] if isinstance(result, tuple) else result
    except Exception as e:
        st.error(f"Redis 讀取失敗: {e}")
        market_guide, date_filtered_df = None, pd.DataFrame()

    # 防呆機制：如果 Airflow 還沒算完該夜市資料
    if not market_guide:
        st.warning(f"⚠️ 尚無 {selected_market} 的預先運算避險指南，請確認 Airflow 排程是否執行完成。")
        return

    # 5. 變數準備
    # 直接從懶人包提取安全指標
    stats = {
        "peak_period": market_guide.get("peak_period", "未知"),
        "rain_increase": market_guide.get("rain_increase", 0),
        "danger_zone": market_guide.get("danger_zone", "未知"),
        "best_entry": market_guide.get("best_entry_name", "未知出口")
    }
    exit_name = stats["best_entry"]
    exit_point = market_guide.get("best_entry_coord", [nm_row["lat"], nm_row["lon"]])
    
    # 組合動態文案
    insight_text = f"整體來看，{selected_market}最高風險集中在 {stats['danger_zone']}，最安全推薦入口為 {exit_name}。雨天事故提升 {stats['rain_increase']}%，建議避開 {stats['peak_period']}。"
    icon = random.choice(["⚠️", "🚨", "❗"])

    # 6. 上半部 UI 渲染：基本資訊
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(f"## 🚶‍♂️ {selected_market} 安心導航指南")
    rating = nm_row.get('nightmarket_rating', '4.0')
    st.markdown(f"**⭐ Google 評分**：{rating} 顆星  |  📍 [點擊開啟 Google Maps 導航](https://www.google.com/maps/search/?api=1&query={nm_row['nightmarket_latitude']},{nm_row['nightmarket_longitude']})")
    st.info("💡 建議交通方式：為了您的安全，建議搭乘大眾運輸，或將車輛停放在周邊 500 公尺外的停車場再步行前往，避開人車交織熱區。")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### ⭐ 行人安全提醒")

    # 佈局：左邊地圖，右邊路線
    col_map, col_route = st.columns([1.5, 1], gap="large")

    # 7. 地圖渲染區塊
    with col_map:
        st.markdown(f"#### ⚡ 區段危險等級 ｜ {selected_market} 夜市推薦入口")
        
        # 定義夜市的四至邊界 (Bounding Box)
        north = nm_row.get("nightmarket_northeast_latitude", nm_row['lat'] + 0.005)
        south = nm_row.get("nightmarket_southwest_latitude", nm_row['lat'] - 0.005)
        east = nm_row.get("nightmarket_northeast_longitude", nm_row['lon'] + 0.005)
        west = nm_row.get("nightmarket_southwest_longitude", nm_row['lon'] - 0.005)

        # 初始化 Folium 地圖
        center = [nm_row["nightmarket_latitude"], nm_row["nightmarket_longitude"]]
        m = folium.Map(location=center, zoom_start=16)
        
        # 畫出夜市範圍藍色框線與中心標記
        bounds = [[south, west], [north, east]]
        folium.Rectangle(bounds=bounds, color="blue", fill=False).add_to(m)
        folium.Marker(center, tooltip=f"{nm_row['nightmarket_name']}（夜市中心）", icon=folium.Icon(color="blue", icon="info-sign")).add_to(m)

        # 篩選嚴格落在此方框內的事故，繪製熱力圖
        if not date_filtered_df.empty:
            accidents_inside = date_filtered_df[(date_filtered_df["latitude"].between(south, north)) & (date_filtered_df["longitude"].between(west, east))]
            heat_data = accidents_inside[["latitude", "longitude"]].values.tolist()
            if heat_data: HeatMap(heat_data, radius=20, blur=15).add_to(m)

        # 8. 呼叫 OSRM 外部 API 計算步行路線
        start, end = f"{center[1]},{center[0]}", f"{exit_point[1]},{exit_point[0]}"
        url = f"http://router.project-osrm.org/route/v1/foot/{start};{end}?overview=full&geometries=polyline&steps=true"
        route_instructions = []

        try:
            res = requests.get(url).json()
            # 將編碼過的路線字串解碼為座標陣列，並畫在地圖上
            route = polyline.decode(res["routes"][0]["geometry"])
            folium.PolyLine(locations=route, color="blue", weight=7, opacity=1).add_to(m)
            
            # 整理路線文字導航 (反轉是因為我們想從入口導航到中心，但 API 是從中心導航到入口)
            steps = list(reversed(res["routes"][0]["legs"][0]["steps"]))

            def translate_maneuver(step, is_first, is_last):
                m = step["maneuver"]
                t, mod = m.get("type", ""), m.get("modifier", "")
                if is_first: return "從推薦入口開始步行"
                if is_last: return "抵達夜市中心"
                if t == "turn": return {"left": "左轉", "right": "右轉", "straight": "直走"}.get(mod, "轉彎")
                if t == "new name": return "沿著道路前進"
                if t == "continue": return "繼續直走"
                return "前進"

            for idx, step in enumerate(steps):
                action = translate_maneuver(step, is_first=(idx == 0), is_last=(idx == len(steps) - 1))
                road = step["name"] if step["name"] != "" else "路線引導"
                if idx == 0 or idx == len(steps) - 1: route_instructions.append(action)
                else: route_instructions.append(f"{action}，沿著 **{road}** 前進 **{int(step['distance'])} 公尺**")
        except:
            # 如果 API 請求失敗，直接畫一條直線
            folium.PolyLine(locations=[center, exit_point], color="blue", weight=7, opacity=1).add_to(m)

        # 調整地圖視野
        m.fit_bounds([[north, east], [north, west], [south, east], [south, west]])
        
        # 繪製自訂的 HTML 氣泡標記，指出最佳入口
        is_north = abs(exit_point[0] - north) < 1e-7
        offset_lat, triangle_position = (-0.00030, "up") if is_north else (0.00030, "down")
        
        html_box = f"""
        <div style="position: relative; background: white; padding: 6px 10px; border-radius: 6px; border: 1.5px solid #333; font-size: 14px; font-weight: bold; color: #222; text-align: center; box-shadow: 0 4px 8px rgba(0,0,0,0.45);">推薦入口
        <div style="position: absolute; {'bottom: -12px' if triangle_position=='down' else 'top: -12px'}; left: 50%; transform: translateX(-50%); width: 0; height: 0; border-left: 10px solid transparent; border-right: 10px solid transparent; border-{'top' if triangle_position=='down' else 'bottom'}: 12px solid white; filter: drop-shadow(0 {'-2px' if triangle_position=='down' else '2px'} 2px rgba(0,0,0,0.3));"></div></div>
        """
        folium.Marker(location=[exit_point[0] + offset_lat, exit_point[1]], icon=DivIcon(icon_size=(150, 40), icon_anchor=(75, 0), html=html_box)).add_to(m)
        folium.Marker(location=exit_point, icon=DivIcon(icon_size=(30, 30), icon_anchor=(15, 15), html="<div style='width: 30px; height: 30px; border-radius: 50%; background-color: #0096FF; color: white; display: flex; justify-content: center; align-items: center; font-size: 16px; font-weight: bold;'>i</div>")).add_to(m)
        
        # 渲染地圖
        st_folium(m, height=450, use_container_width=True, returned_objects=[])

    # 9. 路線文字說明區塊
    with col_route:
        st.markdown("#### 🧭 路線說明（步行）")
        if route_instructions:
            for i, inst in enumerate(route_instructions, 1):
                st.markdown(f"<div style='background-color: #f4f6f9; padding: 12px 15px; border-radius: 6px; color: #333; font-weight: 500; margin-bottom: 10px; border-left: 4px solid #e11d48; font-size: 15px;'>《 {i}. {inst} 》</div>", unsafe_allow_html=True)
        else:
            st.info("無法取得路線規劃建議。")
       
    # 10. 底部指標卡片與總結洞察
    def card(title, content, color):
        st.markdown(f"<div style='padding: 16px; border-radius: 12px; background-color: {color}; color: white; box-shadow: 0 4px 8px rgba(0,0,0,0.1); height: 100%;'><h4 style='margin-top: 0; margin-bottom: 10px; color:white;'>{title}</h4><p style='font-size: 14px; line-height: 1.5; margin-bottom: 0;'>{content}</p></div>", unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1: card("🟥 事故時段", f"最危險時段：<b>{stats['peak_period']}</b><br>建議避開此尖峰。", "#E74C3C")
    with c2: card("🟦 事故原因", f"雨天事故提升 <b>{stats['rain_increase']}%</b><br>濕滑與視線不良為主因。", "#3498DB")
    with c3: card("🟨 區域風險", f"事故最高發區域：<b>{stats['danger_zone']}</b><br>建議避免穿越該區。", "#F1C40F")
    with c4: card("🟩 安全建議", f"建議從 <b>{stats['best_entry']}</b> 進入<br>路幅較寬，人車分流佳。", "#27AE60")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(f"<div style='background-color: #f8f9fa; padding: 15px 20px; border-radius: 10px; border-left: 6px solid #4a90e2; font-size: 16px; color: #333; box-shadow: 0 2px 5px rgba(0,0,0,0.05);'><b style='color:#222;'>{icon} 洞察：</b><span style='color:#222;'>{insight_text}</span></div>", unsafe_allow_html=True)
    st.markdown("---")

# 頁面進入點
if __name__ == "__main__":
    st.set_page_config(layout="wide", page_title="行人看這裡", page_icon="🚶")
    ui.render_sidebar(ds.get_all_nightmarkets())
    ui.load_custom_css()
    act3_render()