from utils.sidebar import sidebar_filters
from folium.plugins import HeatMap
from streamlit_folium import st_folium
from folium.features import DivIcon
from data_loader import load_night_markets,load_accidents,load_env_factors,load_human_factors
import numpy as np
import random
import folium
import streamlit as st
import pandas as pd
import utils.market_tools as mt
import requests
import polyline

# -----------------------------------------------------
# nm_df (夜市資料) - DataFrame
# -----------------------------------------------------
nm_df = load_night_markets()

# -----------------------------------------------------
# accidents_df 事故資料（事故主表）- DataFrame
# -----------------------------------------------------
accidents_df = load_accidents(columns="accident_id, latitude, longitude, death_count, injury_count, accident_datetime, weather_condition")

accidents_df["accident_datetime"] = pd.to_datetime(accidents_df["accident_datetime"])
accidents_df["accident_weekday"] = accidents_df["accident_datetime"].dt.weekday
accidents_df["accident_hour"] = accidents_df["accident_datetime"].dt.hour

# -----------------------------------------------------
# human_factors_df（人為因素）- DataFrame
# -----------------------------------------------------
human_factors_df = load_human_factors(columns="accident_id, cause_analysis_major_primary")

# -----------------------------------------------------
# road_factors_df（肇事道路因素）- DataFrame
# -----------------------------------------------------
road_factors_df = load_env_factors(columns="accident_id, road_surface_condition")


# ---------------------------------------------------------
# Act3 主頁面
# ---------------------------------------------------------
def act3_render():

    # ⭐ 每次進入章節時清掉舊資料（避免AI詢問資料重複累積）
    if "page_data" in st.session_state:
        st.session_state["page_data"].clear()
    # -----------------------------------------------------
    # 定義兩個函式給 sidebar_filters 使用
    # -----------------------------------------------------
    def load_city_list():
        return nm_df["nightmarket_city"].drop_duplicates().tolist()

    def load_market_by_city(city):
        return nm_df[nm_df["nightmarket_city"] == city]["nightmarket_name"].tolist()
    
    # -----------------------------------------------------
    # 篩選縣市、夜市、日期篩選資料和日期類型
    # -----------------------------------------------------
    # 1. 取得 sidebar 的篩選結果
    selected_city, selected_market, date_filtered_df, date_filter_type, date_info = sidebar_filters(
    load_city_list,
    load_market_by_city,
    accidents_df
)

    # 2. 在這裡建立 filters（使用者選的條件）
    filters = {
    "city": selected_city,
    "market": selected_market,
    "filter_type": date_filter_type,
    **date_info
}
    # -----------------------------------------------------
    # 依照側邊欄選擇的夜市取得資料
    # -----------------------------------------------------
    nm_match = nm_df[nm_df["nightmarket_name"] == selected_market]

    if nm_match.empty:
        st.error(f"找不到夜市：{selected_market}，請重新選擇。")
        return

    nm_row = nm_match.iloc[0]

    # -----------------------------
    # 頁首：先算資料（不顯示 UI）
    # -----------------------------
    north = nm_row["nightmarket_northeast_latitude"]
    south = nm_row["nightmarket_southwest_latitude"]
    east = nm_row["nightmarket_northeast_longitude"]
    west = nm_row["nightmarket_southwest_longitude"]

    grid_size = 3

    lat_bins = np.linspace(south, north, grid_size + 1)
    lon_bins = np.linspace(west, east, grid_size + 1)

    date_filtered_df["lat_bin"] = pd.cut(date_filtered_df["latitude"], bins=lat_bins, labels=False, include_lowest=True)
    date_filtered_df["lon_bin"] = pd.cut(date_filtered_df["longitude"], bins=lon_bins, labels=False, include_lowest=True)

    date_filtered_df["risk_score"] = date_filtered_df["death_count"] * 3 + date_filtered_df["injury_count"]

    grid_list = []
    for i in range(grid_size):
        for j in range(grid_size):
            cell = date_filtered_df[(date_filtered_df["lat_bin"] == i) & (date_filtered_df["lon_bin"] == j)]
            score = cell["risk_score"].sum()

            if score >= 6:
                color = "red"
            elif score >= 3:
                color = "yellow"
            else:
                color = "green"

            grid_list.append({
                "grid_row": i,
                "grid_col": j,
                "score": score,
                "color": color,
                "accident_count": len(cell)
            })

    grid_df = pd.DataFrame(grid_list)

    # 推薦入口
    accidents_inside = date_filtered_df[
        (date_filtered_df["latitude"]  >= south) &
        (date_filtered_df["latitude"]  <= north) &
        (date_filtered_df["longitude"] >= west) &
        (date_filtered_df["longitude"] <= east)
    ]

    if len(accidents_inside) == 0:
        accidents_inside = date_filtered_df.copy()

    north_mid = [north, (west + east) / 2]
    south_mid = [south, (west + east) / 2]
    east_mid  = [(south + north) / 2, east]
    west_mid  = [(south + north) / 2, west]

    candidates = {
        "北側出口": north_mid,
        "南側出口": south_mid,
        "東側出口": east_mid,
        "西側出口": west_mid,
        "東北角出口": [north, east],
        "西北角出口": [north, west],
        "東南角出口": [south, east],
        "西南角出口": [south, west],
    }

    def min_dist_to_accident(point):
        return min([
            np.sqrt((point[0] - lat)**2 + (point[1] - lon)**2)
            for lat, lon in accidents_inside[["latitude", "longitude"]].values
        ])

    best_exit = max(candidates.items(), key=lambda x: min_dist_to_accident(x[1]))
    exit_name, exit_point = best_exit

    # -------------------------
    # V3 洞察（使用 session_state）
    # -------------------------

    insight_text = mt.generate_insight_V3( selected_market, grid_df, best_exit )

    icons = ["⚠️", "🚨", "❗"]
    icon = random.choice(icons)

    st.markdown(f"""
    <div style="
        display: inline-block;
        background-color: #f0f2f6;
        padding: 10px 20px;
        border-radius: 30px;
        border-left: 6px solid #4a90e2;
        font-size: 17px;
        color: #222;
        white-space: normal;   /* 自動換行 */
        float: right;
        max-width: 70%;        /* 避免太寬 */
    ">
        <b style="color:#222;">{icon} 洞察：</b>
        <span style="color:#222;">{insight_text}</span>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""<div style="height: 40px;"></div>""", unsafe_allow_html=True)

    # -----------------------------------------------------
    # 3. 正文敘述
    # -----------------------------------------------------
    st.markdown("""
<h2>
行人看這裡：<span style="margin-right:10px;">避開<b style="color:red;">危險</b>？</span> 怎麼做？
</h2>
""", unsafe_allow_html=True)

    st.markdown("""
<hr style="
    border: 0;
    height: 6px;
    background: linear-gradient(90deg, #e53935, #ff8a80);
    border-radius: 3px;
">
""", unsafe_allow_html=True)

    st.markdown("""
<h4>
第三步：<b style="color:#f7c843;">How</b> — 那我該怎麼辦？ <span style="margin-right:10px;">數據</span>會建議
</h4>
""", unsafe_allow_html=True)
        
    st.markdown(f"""
    <div style="font-size: 20px;">
    <div style="
            width: 100%;
            height: 0.5px;
            background: linear-gradient(90deg, white, rgba(0,0,0,0.15));
            border-radius: 3px;
            margin: 8px 0 14px 0;
    "></div>
        在夜市這種人車混雜的環境中，要趨吉避凶，核心原則是實行「<b style="color:yellow;">防禦性走路</b>」，確保自己能被看見並主動預警<b style="color:red;">風險</b>。
    <div style="height: 15px;"></div>
    <div style="font-size: 24px;">
     🚦 <span style="margin-right:10px;">行動？— </span>資料能做什麼？</b>？
    </div>
    <div style="
            background-color: #f0f2f6;
            padding: 15px;
            border-radius: 5px;
            color: #222;
            font-weight: 500;
            margin-top: 10px;
            border-left: 4px solid #ff4b4b;
            font-size: 20px;
        ">
        在夜市這類人車密集度極高的環境中， 資料能幫你「<b style="color:red;">預判風險</b>」。數據分析能找出夜市車禍的規律，幫助管理單位或行人避開<b style="color:red;">危險</b>。
    </div>
    </div>
    """, unsafe_allow_html=True)
 
    # -----------------------------------------------------
    # 區段危險等級
    # -----------------------------------------------------

    st.markdown(""" *** """)
    st.subheader(f" - ⚡ 區段危險等級 ｜ {selected_market} 夜市推薦入口")

    st.markdown("""
    <hr style="
        border: 0;
        height: 4px;
        background: linear-gradient(90deg, #fdd835, #fff59d);
        border-radius: 2px;
    ">
    """, unsafe_allow_html=True)

    # ---------------------------------------------------------
    # 1. 區段危險等級計算（3×3）
    # ---------------------------------------------------------
    north = nm_row["nightmarket_northeast_latitude"]
    south = nm_row["nightmarket_southwest_latitude"]
    east = nm_row["nightmarket_northeast_longitude"]
    west = nm_row["nightmarket_southwest_longitude"]

    grid_size = 3

    lat_bins = np.linspace(south, north, grid_size + 1)
    lon_bins = np.linspace(west, east, grid_size + 1)

    date_filtered_df["lat_bin"] = pd.cut(date_filtered_df["latitude"], bins=lat_bins, labels=False, include_lowest=True)
    date_filtered_df["lon_bin"] = pd.cut(date_filtered_df["longitude"], bins=lon_bins, labels=False, include_lowest=True)

    date_filtered_df["risk_score"] = date_filtered_df["death_count"] * 3 + date_filtered_df["injury_count"]

    grid_list = []
    for i in range(grid_size):
        for j in range(grid_size):
            cell = date_filtered_df[(date_filtered_df["lat_bin"] == i) & (date_filtered_df["lon_bin"] == j)]
            score = cell["risk_score"].sum()

            if score >= 6:
                color = "red"
            elif score >= 3:
                color = "yellow"
            else:
                color = "green"

            grid_list.append({
                "grid_row": i,
                "grid_col": j,
                "score": score,
                "color": color,
                "accident_count": len(cell)
            })

    grid_df = pd.DataFrame(grid_list)

    # ---------------------------------------------------------
    # 2. 畫底圖 + 夜市 bounding box
    # ---------------------------------------------------------
    center = [nm_row["nightmarket_latitude"], nm_row["nightmarket_longitude"]]
    m = folium.Map(location=center, zoom_start=16)

    bounds = [[south, west], [north, east]]
    folium.Rectangle(bounds=bounds, color="blue", fill=False).add_to(m)

    folium.Marker(
        center,
        tooltip=f"{nm_row['nightmarket_name']}（夜市中心）",
        icon=folium.Icon(color="blue", icon="info-sign")
    ).add_to(m)

    # ---------------------------------------------------------
    # 3. 畫 3×3 區塊
    # ---------------------------------------------------------
    lat_step = (north - south) / grid_size
    lon_step = (east - west) / grid_size

    for _, row in grid_df.iterrows():
        i = row["grid_row"]
        j = row["grid_col"]

        south_i = south + i * lat_step
        north_i = south_i + lat_step
        west_j = west + j * lon_step
        east_j = west_j + lon_step

        bounds = [[south_i, west_j], [north_i, east_j]]

        folium.Rectangle(
            bounds=bounds,
            color=row["color"],
            fill=True,
            fill_opacity=0.3,
            tooltip=f"Score: {row['score']} | Accidents: {row['accident_count']}"
        ).add_to(m)

    # ---------------------------------------------------------
    # 4. 畫事故熱區 HeatMap
    # ---------------------------------------------------------
    heat_data = date_filtered_df[["latitude", "longitude"]].values.tolist()
    HeatMap(heat_data, radius=20, blur=15).add_to(m)

    # ---------------------------------------------------------
    # 4-1. 標記每個事故點（CircleMarker）
    # ---------------------------------------------------------
    # for _,（忽略 index）

    # ⭐ 事故點（依死傷程度自動換顏色）
    for _, row in date_filtered_df.iterrows():   

        # 顏色分類
        if row["death_count"] > 0:
            color = "#FF1744"   # 死亡（紅）
        elif row["injury_count"] > 0:
            color = "#0033cc"   # 受傷（藍）
        else:
            color = "#90CAF9"   # 無死傷（淺藍）

        # tooltip（合併你原本的資訊）
        tooltip_text = (
            f"事故ID: {row['accident_id']}<br>"
            f"死傷: {row['death_count']}死 / {row['injury_count']}傷<br>"
            f"時間: {row['accident_datetime']}<br>"
        )

        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=5,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            tooltip=tooltip_text
        ).add_to(m)


    # ---------------------------------------------------------
    # 5. 推薦入口（最遠離事故點）
    # ---------------------------------------------------------

    accidents_inside = date_filtered_df[
        (date_filtered_df["latitude"]  >= south) &
        (date_filtered_df["latitude"]  <= north) &
        (date_filtered_df["longitude"] >= west) &
        (date_filtered_df["longitude"] <= east)
    ]

    if len(accidents_inside) == 0:
        accidents_inside = date_filtered_df.copy()

    north_mid = [north, (west + east) / 2]
    south_mid = [south, (west + east) / 2]
    east_mid  = [(south + north) / 2, east]
    west_mid  = [(south + north) / 2, west]

    candidates = {
        "北側出口": north_mid,
        "南側出口": south_mid,
        "東側出口": east_mid,
        "西側出口": west_mid,
        "東北角出口": [north, east],
        "西北角出口": [north, west],
        "東南角出口": [south, east],
        "西南角出口": [south, west],
    }

    def min_dist_to_accident(point):
        return min([
            np.sqrt((point[0] - lat)**2 + (point[1] - lon)**2)
            for lat, lon in accidents_inside[["latitude", "longitude"]].values
        ])

    best_exit = max(candidates.items(), key=lambda x: min_dist_to_accident(x[1]))
    exit_name, exit_point = best_exit

    center_point = [nm_row["nightmarket_latitude"], nm_row["nightmarket_longitude"]]

    # - 非「原始 Dijkstra」演算法，而是：
    # - Contraction Hierarchies（CH）
    # - A*（A-star）
    # - Bidirectional Dijkstra

    # ---------------------------------------------------------
    # ⭐ 6. OSRM 真實道路路線 + 避開熱點的建議路線
    # ---------------------------------------------------------

    start = f"{center_point[1]},{center_point[0]}"
    end = f"{exit_point[1]},{exit_point[0]}"

    url = f"http://router.project-osrm.org/route/v1/foot/{start};{end}?overview=full&geometries=polyline&steps=true"

    route_instructions = []  # 先給預設，避免失敗時沒變數

    try:
        res = requests.get(url).json()
        route = polyline.decode(res["routes"][0]["geometry"])

        folium.PolyLine(
            locations=route,
            color="blue",
            weight=7,
            opacity=1
        ).add_to(m)

        # 解析 steps
        steps = res["routes"][0]["legs"][0]["steps"]

        # ⭐ 反轉 steps（讓導航從出口走到中心）
        steps = list(reversed(steps))

        def translate_maneuver(step, is_first, is_last):
            m = step["maneuver"]
            t = m.get("type", "")
            mod = m.get("modifier", "")

            # ⭐ 第一個步驟：從推薦入口開始
            if is_first:
                return "從推薦入口開始步行"

            # ⭐ 最後一步：抵達夜市中心
            if is_last:
                return "抵達夜市中心"

            # ⭐ 中間步驟
            if t == "turn":
                if mod == "left":
                    return "左轉"
                if mod == "right":
                    return "右轉"
                if mod == "straight":
                    return "直走"
                return "轉彎"

            if t == "new name":
                return "沿著道路前進"

            if t == "continue":
                return "繼續直走"

            return "前進"

        route_instructions = []
        for idx, step in enumerate(steps):
            action = translate_maneuver(
                step,
                is_first=(idx == 0),
                is_last=(idx == len(steps) - 1)
            )

            road = step["name"] if step["name"] != "" else "路線引導"
            dist = int(step["distance"])

            # ⭐ 第一個步驟：不顯示距離
            if idx == 0:
                route_instructions.append(f"{action}")
                continue

            # ⭐ 最後一步：不顯示距離
            if idx == len(steps) - 1:
                route_instructions.append(f"{action}")
                continue

            # ⭐ 中間步驟：正常顯示距離
            route_instructions.append(f"{action}，沿著 **{road}** 前進 **{dist} 公尺**")


    except Exception as e:
        print("OSRM 路線取得失敗：", e)
        # 如果 OSRM 失敗，退回直線，且不顯示路線說明
        folium.PolyLine(
            locations=[center_point, exit_point],
            color="blue",
            weight=7,
            opacity=1
        ).add_to(m)
        route_instructions = []
        
    # ---------------------------------------------------------
    # ⭐ fit_bounds 使用四角（更穩定）
    # ---------------------------------------------------------
    m.fit_bounds([
        [north, east],
        [north, west],
        [south, east],
        [south, west]
    ])

    # ---------------------------------------------------------
    # ⭐ 自動判斷提示框位置（含小三角形）
    # ---------------------------------------------------------

    is_north = abs(exit_point[0] - north) < 1e-7
    is_south = abs(exit_point[0] - south) < 1e-7

    offset_lat = 0.00030
    triangle_position = "down"

    if is_north:
        offset_lat = -0.00030
        triangle_position = "up"

    if triangle_position == "down":
        html_box = """
    <div style="
        position: relative;
        background: white;
        padding: 8px 12px;
        border-radius: 8px;
        border: 1.5px solid #333;
        font-size: 18px;
        font-weight: bold;
        color: #222;
        text-align: center;
        box-shadow: 0 4px 8px rgba(0,0,0,0.45);
    ">
    夜市推薦入口
    <div style="
        position: absolute;
        bottom: -14px;
        left: 50%;
        transform: translateX(-50%);
        width: 0;
        height: 0;
        border-left: 12px solid transparent;
        border-right: 12px solid transparent;
        border-top: 14px solid white;
        filter: drop-shadow(0 -2px 2px rgba(0,0,0,0.3));
    "></div>
    </div>
    """
    else:
        html_box = """
    <div style="
        position: relative;
        background: white;
        padding: 8px 12px;
        border-radius: 8px;
        border: 1.5px solid #333;
        font-size: 18px;
        font-weight: bold;
        color: #222;
        text-align: center;
        box-shadow: 0 4px 8px rgba(0,0,0,0.45);
    ">
    夜市推薦入口
    <div style="
        position: absolute;
        top: -14px;
        left: 50%;
        transform: translateX(-50%);
        width: 0;
        height: 0;
        border-left: 12px solid transparent;
        border-right: 12px solid transparent;
        border-bottom: 14px solid white;
        filter: drop-shadow(0 2px 2px rgba(0,0,0,0.3));
    "></div>
    </div>
    """

    # ---------------------------------------------------------
    # ⭐ 放置提示框 Marker
    # ---------------------------------------------------------
    folium.Marker(
        location=[exit_point[0] + offset_lat, exit_point[1]],
        icon=DivIcon(
            icon_size=(200, 40),
            icon_anchor=(100, 0),
            html=html_box
        )
    ).add_to(m)

    # ---------------------------------------------------------
    # ⭐ 畫 i 圖示
    # ---------------------------------------------------------
    folium.Marker(
        location=exit_point,
        icon=DivIcon(
            icon_size=(30, 30),
            icon_anchor=(15, 15),
            html="""
            <div style="
                width: 35px;
                height: 35px;
                border-radius: 50%;
                background-color: #0096FF;
                color: white;
                display: flex;
                justify-content: center;
                align-items: center;
                font-size: 18px;
                font-weight: bold;
            ">i
            </div>
            """
        )
    ).add_to(m)

    # ---------------------------------------------------------
    # ⭐ 顯示地圖
    # ---------------------------------------------------------
    st_folium(m, width=800, height=600)

    # ---------------------------------------------------------
    # ⭐ 在地圖下方顯示中文路線說明（如果有）
    # ---------------------------------------------------------

    if route_instructions:
        st.markdown("### 🧭 路線說明（ 步 行 ）")
        for i, inst in enumerate(route_instructions, 1):
            st.markdown(f"""
    <div style="
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 5px;
        color: #222;
        font-weight: 500;
        margin-top: 10px;
        border-left: 4px solid #ff4b4b;
        font-size: 25px;
        ">
        《 {i}. {inst} 》
    </div>
    </div>
        """, unsafe_allow_html=True)
    st.markdown(""" *** """)            
    st.markdown("""
    <div style="
        width: 100%;
        height: 0.5px;
        background-color: white;
        opacity: 0.5;
        border-radius: 0px;
    "></div>
    """, unsafe_allow_html=True)

    # -----------------------------------------------------
    # 行人安全提醒
    # -----------------------------------------------------
    st.header("⭐ 行人安全提醒")

    # -----------------------------------------------------
    # 四宮格資訊計算（只算時段/雨天/區域，不決定入口）
    # -----------------------------------------------------
    def filter_accidents_for_nightmarket(nm_row, date_filtered_df):
        lat_min = nm_row["nightmarket_southwest_latitude"]
        lat_max = nm_row["nightmarket_northeast_latitude"]
        lon_min = nm_row["nightmarket_southwest_longitude"]
        lon_max = nm_row["nightmarket_northeast_longitude"]

        df = date_filtered_df[
            (date_filtered_df["latitude"] >= lat_min) &
            (date_filtered_df["latitude"] <= lat_max) &
            (date_filtered_df["longitude"] >= lon_min) &
            (date_filtered_df["longitude"] <= lon_max)
        ]
        return df

    def compute_safety_stats(nm_row, date_filtered_df, human_factors_df, road_factors_df):
        df = filter_accidents_for_nightmarket(nm_row, date_filtered_df)

        if df.empty:
            return {
                "peak_period": "無事故資料",
                "rain_increase": 0,
                "danger_zone": "無資料",
                "best_entry": None
            }

        # 最危險時段
        peak_hour = df["accident_hour"].value_counts().idxmax()
        peak_period = "20–22 時" if 20 <= peak_hour <= 22 else f"{peak_hour}:00 時段"

        # 雨天事故比例
        merged_weather = df.merge(date_filtered_df, on="accident_id", how="left")
        rain_count = merged_weather[merged_weather["weather_condition"].str.contains("雨", na=False)].shape[0]
        rain_ratio = round((rain_count / len(df)) * 100)

        # 區域風險（8 方向）
        center_lat = nm_row["nightmarket_latitude"]
        center_lon = nm_row["nightmarket_longitude"]

        north = df[df["latitude"] > center_lat].shape[0]
        south = df[df["latitude"] < center_lat].shape[0]
        east  = df[df["longitude"] > center_lon].shape[0]
        west  = df[df["longitude"] < center_lon].shape[0]

        northeast = df[(df["latitude"] > center_lat) & (df["longitude"] > center_lon)].shape[0]
        northwest = df[(df["latitude"] > center_lat) & (df["longitude"] < center_lon)].shape[0]
        southeast = df[(df["latitude"] < center_lat) & (df["longitude"] > center_lon)].shape[0]
        southwest = df[(df["latitude"] < center_lat) & (df["longitude"] < center_lon)].shape[0]

        zone_map = {
            "北側": north,
            "南側": south,
            "東側": east,
            "西側": west,
            "東北側": northeast,
            "西北側": northwest,
            "東南側": southeast,
            "西南側": southwest
        }

        danger_zone = max(zone_map, key=zone_map.get)

        return {
            "peak_period": peak_period,
            "rain_increase": rain_ratio,
            "danger_zone": danger_zone,
            "best_entry": None   # 先佔位，外面再塞真正入口
        }

    stats = compute_safety_stats(
        nm_row,
        date_filtered_df,
        human_factors_df,
        road_factors_df,
    )

    # ⭐ 在這裡統一入口：使用「最遠離事故點」的 exit_name
    stats["best_entry"] = exit_name

    # -----------------------------------------------------
    # 漂亮卡片 UI
    # -----------------------------------------------------
    def card(title, content, color):
        st.markdown(f"""
        <div style="
            padding: 18px;
            border-radius: 12px;
            background-color: {color};
            color: white;
            margin-bottom: 12px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.15);
        ">
            <h3 style="margin-top: 0;">{title}</h3>
            <p style="font-size: 16px; line-height: 1.5;">{content}</p>
        </div>
        """, unsafe_allow_html=True)

    # -----------------------------------------------------
    # 四宮格呈現
    # -----------------------------------------------------
    col1, col2 = st.columns(2)
    col3, col4 = st.columns(2)

    with col1:
        card("🟥 事故時段",
            f"最危險時段：<b>{stats['peak_period']}</b><br>建議避開尖峰時段。",
            "#E74C3C")

    with col2:
        card("🟦 事故原因",
            f"雨天事故提升 <b>{stats['rain_increase']}%</b><br>濕滑路面與視線不良是主因。",
            "#3498DB")

    with col3:
        card("🟨 區域風險",
            f"事故密度最高區域：<b>{stats['danger_zone']}</b><br>建議避免穿越該區域。",
            "#F1C40F")

    with col4:
        card("🟩 安全建議",
            f"建議從 <b>{stats['best_entry']}</b> 進入<br>路幅較寬、人車分流佳。",
            "#27AE60")

        
    st.markdown("""
    <div style="
        width: 100%;
        height: 0.5px;
        background-color: white;
        opacity: 0.5;
        border-radius: 0px;
    "></div>
    """, unsafe_allow_html=True)

    st.markdown(""" *** """)

    # -----------------------------------------------------
    # ⭐ 第三章 AI 所需資訊 (自動化 summary_text)
    # -----------------------------------------------------

    # -----------------------------------------------------
    # ⭐ 產生 V3 5句洞察
    # -----------------------------------------------------
    templates = [insight_text] * 5

    # -----------------------------------------------------
    # ⭐ 把CH3資料放入 session_state
    # -----------------------------------------------------
    st.session_state["ch3"] = {
        "selected_market": selected_market,
        "grid_df": grid_df,
        "exit_name": exit_name,
        "route_instructions": route_instructions,
        "stats": stats,              # ⭐ 新增：行人安全提醒
        "templates": templates
    }
