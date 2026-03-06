from folium.plugins import HeatMap, HeatMapWithTime, MarkerCluster
from streamlit_folium import st_folium
from streamlit.components.v1 import html
from utils.sidebar import sidebar_filters
from data_loader import load_night_markets, load_accidents, load_env_factors, load_human_factors
import streamlit as st
import pandas as pd
from utils.market_tools import generate_insight_V2 as gi
import utils.market_tools as mt
import plotly.express as px
import random
import folium
import datetime

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
human_factors_df = load_human_factors(columns="accident_id, cause_analysis_minor_primary")

# -----------------------------------------------------
# road_factors_df（肇事道路因素）- DataFrame
# -----------------------------------------------------
road_factors_df = load_env_factors(columns="accident_id, light_condition")

# ---------------------------------------------------------
# Act2 主頁面
# ---------------------------------------------------------
def act2_render():

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
    # V2 洞察（generate_insight）
    # -----------------------------------------------------
    insight_text = mt.generate_insight_V2(
    date_filtered_df,
    nm_df,
    selected_market,
    human_factors_df,
    road_factors_df,
    )
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
夜市老實說：<span style="margin-right:10px;">哪些最<b style="color:red;">危險</b>？</span> 為什麼？
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
第二步：<b style="color:#f7c843;">What</b> — 發生了什麼？ <span style="margin-right:10px;">數據</span>會說話
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
        夜市看似熱鬧歡樂，實則潛藏許多交通<b style="color:red;">安全隱憂</b>。夜市環境因人車混合、動線擁擠及駕駛行為等因素，發生車禍的風險往往比一般街道更高！
<div style="height: 15px;"></div>
<div style="font-size: 24px;">
     🚦 <span style="margin-right:10px;">為什麼 — </span>夜市沒你想像中<b style="color:#1e90ff;"> 安全</b>？
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
    夜市隱藏的車禍風險遠比多數人想像中高，主要原因在於其混合動線、環境干擾與高度違規頻率。
</div>
</div>
    """, unsafe_allow_html=True)

    st.markdown(""" *** """)
    st.subheader(f" 夜市相關因素 ")

    st.markdown("""
<hr style="
    border: 0;
    height: 4px;
    background: linear-gradient(90deg, #fdd835, #fff59d);
    border-radius: 2px;
">
""", unsafe_allow_html=True)
    
    # -----------------------------------------------------
    # 夜市相關 — 群聚圖 (Cluster Map) ：白天 / 夜間
    # -----------------------------------------------------
    
    st.markdown(
    f"""
    <h3 style='font-size:25px; font-weight:600; margin-bottom:10px;'>
        📍 {selected_market} ｜ 事故群聚地圖（可切換圖層）
    </h3>
    """,
    unsafe_allow_html=True
)
    st.markdown("""
<div style="
    width: 100%;
    height: 0.5px;
    background: linear-gradient(90deg, white, rgba(255,255,255,0.2));
    border-radius: 3px;
"></div>
""", unsafe_allow_html=True)
    
    # 自訂標題（可完全控制字體大小）
    st.markdown("""
    <div style="
        font-size: 20px;
        font-weight: 800;
        color: #FFD54F;
        margin-bottom: 6px;
    ">
        選擇事故群聚圖類型
    </div>
    """, unsafe_allow_html=True)

    # selectbox 本身不顯示標題
    cluster_mode = st.selectbox(
        "",
        ["☀️ 白天事故群聚", "🌙 夜間事故群聚"]
    )

    # 夜市中心點
    current_nm = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]
    center_lat = current_nm["nightmarket_latitude"]
    center_lon = current_nm["nightmarket_longitude"]

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=17,
        tiles="OpenStreetMap"
    )

    # 夜市圓形 + 標籤
    for _, row in nm_df.iterrows():
        folium.Circle(
            location=[row["nightmarket_latitude"], row["nightmarket_longitude"]],
            radius=180,
            color="#F5A25D",
            weight=2,
            fill=True,
            fill_color="#FDEBD0",
            fill_opacity=0.35,
        ).add_to(m)

        folium.Marker(
            location=[
                row["nightmarket_latitude"] + 0.0006,
                row["nightmarket_longitude"] - 0.0006
            ],
            icon=folium.DivIcon(
                html=f"""
    <div style="
        font-size:20px;
        font-weight:bold;
        color:#FF8C42;
        text-shadow:
            -0.14px -0.14px 0 #000,
            0.14px -0.14px 0 #000,
            -0.14px  0.14px 0 #000,
            0.14px  0.14px 0 #000;
    ">
        {row["nightmarket_name"]}
    </div>
    """
            )
        ).add_to(m)

    # -----------------------------------------------------
    # 日 / 夜事故分組
    # -----------------------------------------------------
 
    def is_night(dt_value):
        dt = pd.to_datetime(dt_value, errors="coerce")
        if pd.isna(dt):
            return False
        hour = dt.hour
        return hour < 6 or hour >= 18

    day_cluster = MarkerCluster()
    night_cluster = MarkerCluster()

    for _, row in date_filtered_df.iterrows():
        tooltip_text = f"""
        <b>事故時間：</b>{row['accident_datetime']}<br>
        <b>死亡：</b>{row['death_count']}<br>
        <b>受傷：</b>{row['injury_count']}<br>
        """

        marker = folium.Marker(
            location=[row["latitude"], row["longitude"]],
            tooltip=tooltip_text,
            icon=folium.Icon(
                color="orange" if not is_night(row["accident_datetime"]) else "blue",
                icon="info-sign"
            )
        )

        if is_night(row["accident_datetime"]):
            marker.add_to(night_cluster)
        else:
            marker.add_to(day_cluster)

    # -----------------------------------------------------
    # ⭐ 根據使用者選擇顯示對應群聚圖
    # -----------------------------------------------------
    if cluster_mode == "☀️ 白天事故群聚":
        day_cluster.add_to(m)
    else:
        night_cluster.add_to(m)

    st_folium(m, width=700, height=450, returned_objects=[])

    # -----------------------------------------------------
    # 夜市相關 — 長期 24 小時事故動畫（HeatMapWithTime）
    # -----------------------------------------------------
    st.markdown(
        f"""
        <h3 style='font-size:25px; font-weight:600; margin-bottom:10px;'>
            🎬 {selected_market} ｜ 長期 24 小時事故熱力動畫（跨年度時段）
        </h3>
        """,
        unsafe_allow_html=True
    )

    # 取得夜市資料
    current_nm = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]

    center_lat = current_nm["nightmarket_latitude"]
    center_lon = current_nm["nightmarket_longitude"]

    # 夜市 bounding box
    ne_lat = current_nm["nightmarket_northeast_latitude"]
    ne_lon = current_nm["nightmarket_northeast_longitude"]
    sw_lat = current_nm["nightmarket_southwest_latitude"]
    sw_lon = current_nm["nightmarket_southwest_longitude"]

    # 事故資料：轉成小時
    date_filtered_df["acc_time"] = pd.to_datetime(date_filtered_df["accident_datetime"])
    date_filtered_df["hour"] = date_filtered_df["acc_time"].dt.hour

    # 只取夜市 bounding box 內的事故
    def inside_bbox(row):
        return (
            (sw_lat <= row["latitude"] <= ne_lat) and
            (sw_lon <= row["longitude"] <= ne_lon)
        )

    nm_acc = date_filtered_df[date_filtered_df.apply(inside_bbox, axis=1)]

    # ⭐ 建立 HeatMapWithTime 資料
    heat_data = []
    time_index = []

    for h in range(24):
        df_h = nm_acc[nm_acc["hour"] == h]

        points = []
        for _, row in df_h.iterrows():
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                points.append([lat, lon])
            except:
                continue

        if len(points) == 1:
            points = points + points

        if len(points) > 0:
            heat_data.append(points)
            time_index.append(f"{h:02d}:00")

    # 建立地圖
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=16.48,
        tiles="OpenStreetMap"
    )

    # 畫出夜市範圍
    folium.Rectangle(
        bounds=[
            [sw_lat, sw_lon],
            [ne_lat, ne_lon]
        ],
        color="#FF8C42",
        weight=2,
        fill=True,
        fill_opacity=0.15
    ).add_to(m)

    # ⭐ HeatMapWithTime
    HeatMapWithTime(
        heat_data,
        index=time_index,
        radius=18,
        auto_play=True,
        max_opacity=0.8
    ).add_to(m)

    # ⭐⭐⭐ 最穩定：用 columns 控制左右空間（動畫不會壞）
    left, center, right = st.columns([0.01, 100, 37])

    with center:
        html(m._repr_html_(), height=420)

    st.markdown(""" *** """)
    st.subheader(f" 事故相關因素 ")
    st.markdown("""
<hr style="
    border: 0;
    height: 4px;
    background: linear-gradient(90deg, #fdd835, #fff59d);
    border-radius: 2px;
">
""", unsafe_allow_html=True)


    # -----------------------------------------------------
    #  事故相關 — 玫瑰圖 / 長條圖切換
    # -----------------------------------------------------   
  
    nm = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]

    lat_min = nm["nightmarket_southwest_latitude"]
    lat_max = nm["nightmarket_northeast_latitude"]
    lon_min = nm["nightmarket_southwest_longitude"]
    lon_max = nm["nightmarket_northeast_longitude"]

    # -----------------------------------------------------
    # 篩選：只保留落在選擇夜市範圍內的事故
    # -----------------------------------------------------
    accidents_in_nm = date_filtered_df[
        (date_filtered_df["latitude"] >= lat_min) &
        (date_filtered_df["latitude"] <= lat_max) &
        (date_filtered_df["longitude"] >= lon_min) &
        (date_filtered_df["longitude"] <= lon_max)
    ]

    human_in_nm = human_factors_df[
        human_factors_df["accident_id"].isin(accidents_in_nm["accident_id"])
    ]

    # -----------------------------------------------------
    # 標題（自訂字體）
    # -----------------------------------------------------
    st.markdown(
    f"""
    <h3 style='font-size:25px; font-weight:600; margin-bottom:10px;'>
        📍 {selected_market} ｜ 事故原因分析（僅夜市範圍）
    </h3>
    """,
    unsafe_allow_html=True
)

    # -----------------------------------------------------
    # 圖表類型切換（selectbox）
    # -----------------------------------------------------
    st.markdown("""
<div style="
        font-size: 20px;
        font-weight: 600;
        color: #FFD54F;
        margin-bottom: 6px;
    ">
        選擇圖表類型
</div>
    """, unsafe_allow_html=True)
    
    
    chart_type = st.selectbox(
        "",
        ["玫瑰圖（Rose Chart）", "水平長條圖"]
    )

    # -----------------------------------------------------
    # 若夜市內沒有事故（人為因素）
    # -----------------------------------------------------
    if human_in_nm.empty:
        st.info(f"{selected_market} 範圍內目前沒有事故資料。")
    else:
        # -----------------------------------------------------
        # 統計事故原因
        # -----------------------------------------------------
        reason_df = (
            human_in_nm["cause_analysis_minor_primary"]
            .value_counts()
            .reset_index()
        )
        reason_df.columns = ["reason", "count"]

        color_map = [
            "#FF6B6B",  # 霓虹紅
            "#FFD93D",  # 霓虹黃
            "#6BCB77",  # 霓虹綠
            "#4D96FF",  # 霓虹藍
            "#9D4EDD",  # 霓虹紫
            "#FF8E3C",  # 霓虹橘
        ]

        # -----------------------------------------------------
        # 玫瑰圖（Polar Rose Chart）
        # -----------------------------------------------------
        if chart_type == "玫瑰圖（Rose Chart）":
            fig = px.bar_polar(
                reason_df,
                r="count",
                theta="reason",
                color="reason",
                color_discrete_sequence=color_map,
                title=f"{selected_market} ｜ 事故原因玫瑰圖"
            )
            fig.update_layout(
                showlegend=False,
                polar=dict(
                    radialaxis=dict(
                        tickfont=dict(color="rgba(0, 0, 0, 1)"),
                        gridcolor="rgba(255,255,255,0.2)",
                        linecolor="rgba(65, 105, 225, 1)"
                    ),
                    angularaxis=dict(
                        tickfont=dict(color="white", size=20)
                    )
                )
            )
            st.plotly_chart(fig, use_container_width=True)

        # -----------------------------------------------------
        # 水平長條圖（barh）
        # -----------------------------------------------------
        else:
            fig = px.bar(
                reason_df,
                y="reason",
                x="count",
                text="count",
                orientation="h",
                color="reason",
                color_discrete_sequence=color_map,
                title=f"{selected_market} ｜ 事故原因水平長條圖"
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(
                yaxis_title="事故原因",
                xaxis_title="事故數量",
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)

    # -----------------------------------------------------
    # 分隔線（不論是否有事故都顯示）
    # -----------------------------------------------------
    st.markdown(""" *** """)

    # -----------------------------------------------------
    # 天氣相關因素（永遠會執行）
    # -----------------------------------------------------
    st.subheader(" 天氣相關因素 ")
    st.markdown("""
    <hr style="
        border: 0;
        height: 4px;
        background: linear-gradient(90deg, #fdd835, #fff59d);
        border-radius: 2px;
    ">
    """, unsafe_allow_html=True)

    
    # -----------------------------------------------------
    # 雨天 vs 非雨天事故比較（長條圖）
    # -----------------------------------------------------
    st.subheader(f"{selected_market}｜雨天 vs 非雨天事故比較（夜市營業時段）")

    mode = st.selectbox(
        "",
        ["事故量比較", "事故嚴重度比較"]
    )

    # 取得夜市 bounding box
    nm_row = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]
    ne_lat = float(nm_row["nightmarket_northeast_latitude"])
    ne_lon = float(nm_row["nightmarket_northeast_longitude"])
    sw_lat = float(nm_row["nightmarket_southwest_latitude"])
    sw_lon = float(nm_row["nightmarket_southwest_longitude"])

    # 過濾事故（夜市範圍）
    market_accidents = accidents_df[
        (accidents_df["latitude"] <= ne_lat) &
        (accidents_df["latitude"] >= sw_lat) &
        (accidents_df["longitude"] <= ne_lon) &
        (accidents_df["longitude"] >= sw_lon)
    ]

    if market_accidents.empty:
        st.warning(f"{selected_market} 範圍內沒有事故資料。")

    else:
        # 轉換事故時間
        market_accidents["accident_datetime"] = pd.to_datetime(
            market_accidents["accident_datetime"], errors="coerce"
        )
        market_accidents["hour"] = market_accidents["accident_datetime"].dt.hour

        # 夜市營業時段（17–23）
        night_df = market_accidents[
            (market_accidents["hour"] >= 17) &
            (market_accidents["hour"] <= 23)
        ]

        if night_df.empty:
            st.warning(f"{selected_market} 在營業時段（17–23）沒有事故資料。")

        else:
            # 雨天 vs 非雨天
            night_df["rain_group"] = night_df["weather_condition"].apply(
                lambda w: "雨天" if isinstance(w, str) and "雨" in w else "非雨天"
            )

            # 事故嚴重度
            night_df["avg_severity"] = (
                night_df["death_count"] * 3 + night_df["injury_count"]
            )

            # -------------------------
            # 事故量比較
            # -------------------------
            if mode == "事故量比較":
                rain_count = (
                    night_df["rain_group"]
                    .value_counts(dropna=False)
                    .reset_index()
                )
                rain_count.columns = ["weather", "count"]

                fig = px.bar(
                    rain_count,
                    x="count",
                    y="weather",
                    orientation="h",
                    text="count",
                    color="weather",
                    labels={"weather": "天氣", "count": "數量"},
                    color_discrete_sequence=["#4D96FF", "#FF6B6B"],
                    title=f"{selected_market}｜雨天 vs 非雨天事故量"
                )
                st.plotly_chart(fig, use_container_width=True)

            # -------------------------
            # 事故嚴重度比較
            # -------------------------
            else:
                severity_stats = (
                    night_df.groupby("rain_group")["avg_severity"]
                    .mean()
                    .reset_index()
                )
                severity_stats.columns = ["weather", "avg_severity"]
                severity_stats["avg_severity"] = severity_stats["avg_severity"].round(1)

                fig = px.bar(
                    severity_stats,
                    x="weather",
                    y="avg_severity",
                    text="avg_severity",
                    color="weather",
                    labels={"weather": "天氣", "avg_severity": "平均嚴重度"},
                    color_discrete_sequence=["#4D96FF", "#FF6B6B"],
                    title=f"{selected_market}｜雨天 vs 非雨天事故嚴重度"
                )
                st.plotly_chart(fig, use_container_width=True)

            st.markdown(""" *** """)

    # -----------------------------------------------------
    # 雨天事故熱點
    # -----------------------------------------------------
    st.subheader(f"🌧️ {selected_market}｜雨天事故熱點（含夜市範圍）")

    # 統一 accident_id 型別
    accidents_df["accident_id"] = accidents_df["accident_id"].astype(str)

    # 取得夜市 bounding box
    nm_row = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]

    ne_lat = nm_row["nightmarket_northeast_latitude"]
    ne_lon = nm_row["nightmarket_northeast_longitude"]
    sw_lat = nm_row["nightmarket_southwest_latitude"]
    sw_lon = nm_row["nightmarket_southwest_longitude"]

    center_lat = nm_row["nightmarket_latitude"]
    center_lon = nm_row["nightmarket_longitude"]

    # 篩出該夜市事故（用 accidents_df）
    market_accidents = accidents_df[
        (accidents_df["latitude"] <= ne_lat) &
        (accidents_df["latitude"] >= sw_lat) &
        (accidents_df["longitude"] <= ne_lon) &
        (accidents_df["longitude"] >= sw_lon)
    ]

    if market_accidents.empty:
        st.warning(f"{selected_market} 範圍內沒有事故資料。")

    else:
        # 判斷雨天
        market_accidents["rain_group"] = market_accidents["weather_condition"].apply(
            lambda w: "雨天" if isinstance(w, str) and ("雨" in w) else "非雨天"
        )

        # 篩出雨天事故
        rain_df = market_accidents[market_accidents["rain_group"] == "雨天"]

        if rain_df.empty:
            st.warning(f"{selected_market} 在雨天沒有事故資料。")

        else:
            fig = px.density_mapbox(
                rain_df,
                lat="latitude",
                lon="longitude",
                radius=40,
                center={"lat": center_lat, "lon": center_lon},
                zoom=16,
                mapbox_style="carto-positron",
                color_continuous_scale="Turbo",
                height=450,
            )

            fig.add_scattermapbox(
                lat=[center_lat],
                lon=[center_lon],
                mode="markers+text",
                text=[selected_market],
                textposition="top center",
                marker=dict(size=12, color="red"),
            )

            # 夜市範圍框線
            fig.add_scattermapbox(
                lat=[ne_lat, ne_lat, sw_lat, sw_lat, ne_lat],
                lon=[ne_lon, sw_lon, sw_lon, ne_lon, ne_lon],
                mode="lines",
                line=dict(width=3, color="red"),
            )

            fig.update_layout(
                margin=dict(l=0, r=0, t=40, b=0),
                showlegend=False
            )

            st.plotly_chart(fig, use_container_width=True)

    st.markdown(""" *** """)


            
    # -----------------------------------------------------
    # 夜市營業時段｜天氣風險雷達圖
    # -----------------------------------------------------

    st.subheader(f"🧭{selected_market}｜夜市營業時段天氣風險雷達圖")

    # 取得夜市 bounding box
    nm = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]

    lat_min = nm["nightmarket_southwest_latitude"]
    lat_max = nm["nightmarket_northeast_latitude"]
    lon_min = nm["nightmarket_southwest_longitude"]
    lon_max = nm["nightmarket_northeast_longitude"]

    # 篩選夜市事故（直接從 date_filtered_df）
    acc_nm = date_filtered_df[
        (date_filtered_df["latitude"] >= lat_min) &
        (date_filtered_df["latitude"] <= lat_max) &
        (date_filtered_df["longitude"] >= lon_min) &
        (date_filtered_df["longitude"] <= lon_max)
    ].copy()

    # 若夜市內沒有事故
    if acc_nm.empty:
        st.info(f"{selected_market} 在營業時段沒有事故資料。")
        st.stop()

    # accident_id 型別一致
    acc_nm["accident_id"] = acc_nm["accident_id"].astype(int)
    road_factors_df["accident_id"] = road_factors_df["accident_id"].astype(int)
    human_factors_df["accident_id"] = human_factors_df["accident_id"].astype(int)

    # ⭐ df 就是 acc_nm，不要再 merge date_filtered_df（避免覆蓋欄位）
    df = acc_nm.copy()

    # 合併道路資料
    df = df.merge(road_factors_df, on="accident_id", how="left")

    # 避免 KeyError
    if "road_surface_condition" not in df.columns:
        df["road_surface_condition"] = None
    if "light_condition" not in df.columns:
        df["light_condition"] = None
    if "weather_condition" not in df.columns:
        df["weather_condition"] = None

    # 取夜市營業時段（17–23）
    df["accident_datetime"] = pd.to_datetime(df["accident_datetime"], errors="coerce")
    df["hour"] = df["accident_datetime"].dt.hour
    df_night = df[(df["hour"] >= 17) & (df["hour"] <= 23)]

    if df_night.empty:
        st.info(f"{selected_market} 在營業時段沒有事故資料。")
        st.stop()

    # -----------------------------------------------------
    # 建立天氣風險指標
    # -----------------------------------------------------

    df_night["is_rain"] = df_night["weather_condition"].apply(
        lambda w: 1 if isinstance(w, str) and ("雨" in w) else 0
    )

    df_night["is_dark"] = df_night["light_condition"].apply(
        lambda x: 1 if isinstance(x, str) and (
            "暗" in x or "夜" in x or "未開啟" in x or "無照明" in x
        ) else 0
    )

    df_night["is_wet"] = df_night["road_surface_condition"].apply(
        lambda r: 1 if isinstance(r, str) and ("濕" in r or "積水" in r) else 0
    )

    # -----------------------------------------------------
    # 計算比例
    # -----------------------------------------------------
    risk_stats = {
        "雨天": df_night["is_rain"].mean(),
        "光線不佳": df_night["is_dark"].mean(),
        "濕滑路面": df_night["is_wet"].mean(),
    }

    risk_df = pd.DataFrame({
        "risk": list(risk_stats.keys()),
        "value": [round(v * 100, 1) for v in risk_stats.values()]
    })

    # -----------------------------------------------------
    # 雷達圖
    # -----------------------------------------------------
    fig = px.line_polar(
        risk_df,
        r="value",
        theta="risk",
        line_close=True,
        markers=True,
        range_r=[0, max(risk_df["value"]) * 1.2],
    )

    fig.update_traces(fill="toself", marker=dict(color="#FF6B6B", size=10))
    fig.update_layout(
        showlegend=False,
        polar=dict(
            radialaxis=dict(
                tickfont=dict(color="#000000"),
                gridcolor="rgba(255,255,255,0.2)",
                linecolor="rgba(65, 105, 225, 1)"
            ),
            angularaxis=dict(
                tickfont=dict(color="#FFFFFF", size=20)
            )
        )
    )
    st.markdown(""" *** """)

    # -----------------------------------------------------
    # ⭐ 第二章 AI 所需資訊 (自動化 summary_text)
    # -----------------------------------------------------

    df = date_filtered_df
    total = len(df)

    # 死亡 / 受傷
    def safe_sum(col):
        return df[col].sum() if col in df.columns else 0

    deaths = safe_sum("死亡人數")
    injuries = safe_sum("受傷人數")

    # 最危險路段
    if "street" in df.columns and total > 0:
        top_street = df["street"].mode()[0]
        top_street_count = df["street"].value_counts().max()
    else:
        top_street = "無資料"
        top_street_count = 0

    # 最危險時段
    def hour_to_label(h):
        if 5 <= h < 8: return "清晨"
        if 8 <= h < 12: return "上午"
        if 12 <= h < 17: return "下午"
        if 17 <= h < 20: return "傍晚"
        return "深夜"

    if "accident_hour" in df.columns and total > 0:
        peak_hour = df["accident_hour"].mode()[0]
        time_label = hour_to_label(peak_hour)
    else:
        peak_hour = "無資料"
        time_label = "無資料"

    # 夜市危險指數（PDI）
    danger_weight = deaths * 5 + injuries * 2
    pdi = round((total * danger_weight) / 10, 2) if total else 0

    # -----------------------------------------------------
    # ⭐ 產生 V2 5句洞察
    # -----------------------------------------------------
    templates = [insight_text] * 5

    # -----------------------------------------------------
    # ⭐ 存入 session_state
    # -----------------------------------------------------
    st.session_state["ch2"] = {
        "selected_market": selected_market,
        "templates": templates,
    }


