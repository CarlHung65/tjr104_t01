from folium.plugins import HeatMapWithTime, MarkerCluster
from streamlit_folium import st_folium
from streamlit.components.v1 import html
from utils.sidebar import sidebar_filters
from data_loader import load_night_markets, load_env_factors, load_human_factors, load_accidents
from utils.maps_tool import build_map_cluster
from utils.charts_tool import build_cause_charts, build_rain_comparison_charts, build_weather_radar_chart

import streamlit as st
import pandas as pd
import utils.market_tools as mt
import plotly.express as px
import random
import calendar

# -----------------------------------------------------
# nm_df (夜市資料) - DataFrame
# -----------------------------------------------------
nm_df = load_night_markets()

# ---------------------------------------------------------
# Act2 主頁面
# ---------------------------------------------------------
def act2_render():

    if st.session_state.page != "夜市老實說":
        return

    # ⭐ 每次進入章節時清掉舊資料
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
    # ⭐ 取得 sidebar 的篩選結果
    # -----------------------------------------------------
    selected_city, selected_market, date_filter_type, date_info = sidebar_filters(
        load_city_list,
        load_market_by_city,
    )

    # -----------------------------------------------------
    # ⭐ 查事故主表（智慧 LIMIT）
    # -----------------------------------------------------
    if date_filter_type == "月份":
        year = date_info["year"]
        month = date_info["month"]
        last_day = calendar.monthrange(year, month)[1]

        date_filtered_df = load_accidents(
            start_date=f"{year}-{month:02d}-01",
            end_date=f"{year}-{month:02d}-{last_day:02d}",
            date_filter_type=date_filter_type
        )

    elif date_filter_type == "單一日期":
        date = date_info["date"]
        date_filtered_df = load_accidents(
            start_date=date,
            end_date=date,
            date_filter_type=date_filter_type
        )

    elif date_filter_type == "日期區間（最新7天）":
        start_date = date_info["start_date"]
        end_date = date_info["end_date"]
        date_filtered_df = load_accidents(
            start_date=start_date,
            end_date=end_date,
            date_filter_type=date_filter_type
        )

    elif date_filter_type == "年份（選月份）":
        year = date_info["year"]
        month = date_info["month"]
        last_day = calendar.monthrange(year, month)[1]

        date_filtered_df = load_accidents(
            start_date=f"{year}-{month:02d}-01",
            end_date=f"{year}-{month:02d}-{last_day:02d}",
            date_filter_type=date_filter_type
        )

    # -----------------------------------------------------
    # ⭐ 事故主表已經有了 → 取得 accident_id
    # -----------------------------------------------------
    acc_ids = date_filtered_df["accident_id"].tolist()

    # -----------------------------------------------------
    # ⭐ 撈人為因素（只撈主表 accident_id）
    # -----------------------------------------------------
    human_factors_df = load_human_factors(
        columns="accident_id, cause_analysis_minor_primary",
        accident_ids=acc_ids
    )

    # -----------------------------------------------------
    # ⭐ 撈道路環境因素（只撈主表 accident_id）
    # -----------------------------------------------------
    road_factors_df = load_env_factors(
        columns="accident_id, light_condition",
        accident_ids=acc_ids
    )

    # -----------------------------------------------------
    # ⭐ 後面你的圖表、敘述、AI 分析全部照舊使用 date_filtered_df
    # -----------------------------------------------------
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
    # 夜市事故群聚圖（最佳化版本）
    # -----------------------------------------------------

    st.markdown(
        f"""
        <h3 style='font-size:25px; font-weight:600; margin-bottom:10px;'>
            📍 {selected_market} ｜ 事故群聚地圖（可切換圖層）
        </h3>
        """,
        unsafe_allow_html=True
    )

    cluster_mode = st.selectbox("", ["☀️ 白天事故群聚", "🌙 夜間事故群聚"])

    # ⭐ 呼叫新的 build_map_cluster（專門給 act2）
    m = build_map_cluster(
        selected_market=selected_market,
        date_filtered_df=date_filtered_df,
        nm_df=nm_df,
        cluster_mode=cluster_mode
    )

    st_folium(m, width=700, height=450, returned_objects=[])
    
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
    # 事故原因分析（玫瑰圖/長條圖）
    # -----------------------------------------------------
    build_cause_charts(
        selected_market=selected_market,
        date_filtered_df=date_filtered_df,
        nm_df=nm_df,
        human_factors_df=human_factors_df
    )
    
    # -----------------------------------------------------
    # 雨天 vs 非雨天事故比較
    # -----------------------------------------------------
    build_rain_comparison_charts(
        selected_market=selected_market,
        date_filtered_df=date_filtered_df,
        nm_df=nm_df
    )
            
    # -----------------------------------------------------
    # 夜市營業時段｜天氣風險雷達圖
    # -----------------------------------------------------
    build_weather_radar_chart(
        selected_market=selected_market,
        date_filtered_df=date_filtered_df,
        nm_df=nm_df,
        road_factors_df=road_factors_df
    )

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

    
    # -----------------------------------------------------
    # 夜市相關 — 長期 24 小時事故動畫（太耗資源）
    # -----------------------------------------------------

    # st.markdown(
    #     f"""
    #     <h3 style='font-size:25px; font-weight:600; margin-bottom:10px;'>
    #         🎬 {selected_market} ｜ 長期 24 小時事故熱力動畫（跨年度時段）
    #     </h3>
    #     """,
    #     unsafe_allow_html=True
    # )

    # # 夜市資料
    # nm = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]
    # center_lat, center_lon = nm["nightmarket_latitude"], nm["nightmarket_longitude"]

    # ne_lat, ne_lon = nm["nightmarket_northeast_latitude"], nm["nightmarket_northeast_longitude"]
    # sw_lat, sw_lon = nm["nightmarket_southwest_latitude"], nm["nightmarket_southwest_longitude"]

    # -----------------------------------------------------
    # ⭐ 1. 夜市範圍事故（只做一次）
    # -----------------------------------------------------
    # nm_acc = date_filtered_df[
    #     (date_filtered_df["latitude"] >= sw_lat) &
    #     (date_filtered_df["latitude"] <= ne_lat) &
    #     (date_filtered_df["longitude"] >= sw_lon) &
    #     (date_filtered_df["longitude"] <= ne_lon)
    # ].copy()

    # -----------------------------------------------------
    # ⭐ 2. 時間處理（只做一次）
    # -----------------------------------------------------
    # nm_acc["accident_datetime"] = pd.to_datetime(nm_acc["accident_datetime"], errors="coerce")
    # nm_acc["hour"] = nm_acc["accident_datetime"].dt.hour

    # -----------------------------------------------------
    # ⭐ 3. HeatMapWithTime 資料（不再用 iterrows）
    # -----------------------------------------------------
    # heat_data = [
    #     nm_acc[nm_acc["hour"] == h][["latitude", "longitude"]].values.tolist()
    #     for h in range(24)
    # ]
    # time_index = [f"{h:02d}:00" for h in range(24)]

    # -----------------------------------------------------
    # ⭐ 4. 地圖
    # -----------------------------------------------------
    # m = folium.Map(
    #     location=[center_lat, center_lon],
    #     zoom_start=16.48,
    #     tiles="OpenStreetMap"
    # )

    # 夜市框線
    # folium.Rectangle(
    #     bounds=[[sw_lat, sw_lon], [ne_lat, ne_lon]],
    #     color="#FF8C42",
    #     weight=2,
    #     fill=True,
    #     fill_opacity=0.15
    # ).add_to(m)

    # HeatMapWithTime
    # HeatMapWithTime(
    #     heat_data,
    #     index=time_index,
    #     radius=18,
    #     auto_play=True,
    #     max_opacity=0.8
    # ).add_to(m)

    # # 顯示
    # left, center, right = st.columns([0.01, 100, 37])
    # with center:
    #     html(m._repr_html_(), height=420)


