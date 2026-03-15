from utils.sidebar import sidebar_filters
from folium.plugins import HeatMap
from streamlit_folium import st_folium
<<<<<<< HEAD
from data_loader import load_night_markets, load_accidents, load_env_factors
=======
from data_loader import load_night_markets, load_env_factors, load_accidents
from utils.maps_tool import build_taiwan_density_map
>>>>>>> Tom
import pandas as pd
import streamlit as st
import utils.market_tools as mt
import altair as alt
<<<<<<< HEAD
import random
import folium
=======
import numpy as np
import random
import calendar
>>>>>>> Tom

# -----------------------------------------------------
# nm_df (夜市資料) - DataFrame
# -----------------------------------------------------
nm_df = load_night_markets()

<<<<<<< HEAD
# -----------------------------------------------------
# accidents_df 事故資料（事故主表）- DataFrame
# -----------------------------------------------------
accidents_df = load_accidents(columns="accident_id, latitude, longitude, death_count, injury_count, accident_datetime ,weather_condition")

accidents_df["accident_datetime"] = pd.to_datetime(accidents_df["accident_datetime"])
accidents_df["accident_weekday"] = accidents_df["accident_datetime"].dt.weekday
accidents_df["accident_hour"] = accidents_df["accident_datetime"].dt.hour

# -----------------------------------------------------
# road_factors_df（肇事道路因素）- DataFrame
# -----------------------------------------------------
road_factors_df = load_env_factors(columns="accident_id, light_condition")

=======
>>>>>>> Tom
# ---------------------------------------------------------
# Act4 主頁面
# ---------------------------------------------------------
def act4_render():

<<<<<<< HEAD
    # ⭐ 每次進入章節時清掉舊資料（避免AI詢問資料重複累積）
=======
    if st.session_state.page != "政府幫幫忙":
        return

    # ⭐ 每次進入章節時清掉舊資料
>>>>>>> Tom
    if "page_data" in st.session_state:
        st.session_state["page_data"].clear()

    # -----------------------------------------------------
    # 定義兩個函式給 sidebar_filters 使用
    # -----------------------------------------------------
    def load_city_list():
        return nm_df["nightmarket_city"].drop_duplicates().tolist()

    def load_market_by_city(city):
        return nm_df[nm_df["nightmarket_city"] == city]["nightmarket_name"].tolist()
<<<<<<< HEAD
    

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
    # date_filtered_df 導入 light_condition
    # -----------------------------------------------------
    date_filtered_df = date_filtered_df.merge(
    road_factors_df,
    on="accident_id",
    how="left"
=======

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
    # ⭐ 導入 light_condition（你原本的邏輯）
    # -----------------------------------------------------
    date_filtered_df = date_filtered_df.merge(
        road_factors_df,
        on="accident_id",
        how="left"
>>>>>>> Tom
    )

    # -----------------------------
    # 頁首：先算資料（不顯示 UI）
    # -----------------------------
    def calculate_city_rank(nm_df, date_filtered_df):

        WEIGHT_DEATH = 5
        WEIGHT_INJURY = 2
        WEIGHT_OPEN = 3
        WEIGHT_CLOSE = 1

        date_filtered_df["accident_datetime"] = pd.to_datetime(date_filtered_df["accident_datetime"])

        pdi_rows = []
        for _, nm in nm_df.iterrows():
            nm_name = nm["nightmarket_name"]

            lat_min = nm["nightmarket_southwest_latitude"]
            lat_max = nm["nightmarket_northeast_latitude"]
            lon_min = nm["nightmarket_southwest_longitude"]
            lon_max = nm["nightmarket_northeast_longitude"]

            acc_in_nm = date_filtered_df[
                (date_filtered_df["latitude"] >= lat_min) &
                (date_filtered_df["latitude"] <= lat_max) &
                (date_filtered_df["longitude"] >= lon_min) &
                (date_filtered_df["longitude"] <= lon_max)
            ]

            total_pdi = 0
            for _, acc in acc_in_nm.iterrows():
                hour = acc["accident_datetime"].hour
                severity = acc["death_count"] * WEIGHT_DEATH + acc["injury_count"] * WEIGHT_INJURY
                weight = WEIGHT_OPEN if (hour >= 17 or hour == 0) else WEIGHT_CLOSE
                total_pdi += severity * weight

            pdi_rows.append({
                "nightmarket_name": nm_name,
                "accident_count": len(acc_in_nm),
                "pdi": total_pdi
            })

        pdi_raw = pd.DataFrame(pdi_rows)

        pdi_raw["nightmarket_name"] = pdi_raw["nightmarket_name"].str.strip()
        nm_df["nightmarket_name"] = nm_df["nightmarket_name"].str.strip()

        pdi_raw = pdi_raw.merge(
            nm_df[["nightmarket_name", "nightmarket_city"]],
            on="nightmarket_name",
            how="left"
        )

        city_rank = pdi_raw.groupby("nightmarket_city").agg(
            夜市數量=("nightmarket_name", "count"),
            事故總數=("accident_count", "sum"),
            PDI總和=("pdi", "sum"),
        ).reset_index()

        city_rank = city_rank.rename(columns={"nightmarket_city": "城市"})
        city_rank["平均PDI"] = (city_rank["PDI總和"] / city_rank["夜市數量"]).round(1)

        def danger_level(pdi):
            if pdi <= 10:
                return "🟢 安全"
            elif pdi <= 30:
                return "🟡 注意"
            elif pdi <= 60:
                return "🟠 危險"
            else:
                return "🔴 極危險"

        city_rank["危險等級"] = city_rank["平均PDI"].apply(danger_level)

        return city_rank
    
    # -----------------------------------------------------
    # V4 洞察（generate_insight）
    # -----------------------------------------------------
    city_rank = calculate_city_rank(nm_df, date_filtered_df)
    insight_text = mt.generate_insight_V4(city_rank)

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
政府幫幫忙：<span style="margin-right:10px;">改善？？<b style="color:red;"></b></span> <b style="color:yellow;">政府</b> 該怎做？
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
第四步：<b style="color:#f7c843;">How to do？</b> — 政策如何落實？ <span style="margin-right:10px;">數據</span>指明燈
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
        改善夜市交通安全應採取「<b style="color:yellow;">3E 政策</b>」（工程、教育、執法），並結合最新的「人本交通」趨勢。
    <div style="height: 15px;"></div>
    <div style="font-size: 24px;">
     🚦 <span style="margin-right:10px;">行人至上(？) </span>政府怎協助？</b>？
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
        有效改善夜市周邊的交通安全並減少<b style="color:red;">車禍</b>，政府需從交通工程改善、管理機制建立、以及宣導執法三個面向著手，核心目標是推動「<b style="color:green;">人本交通</b>」。
    </div>
    </div>
    """, unsafe_allow_html=True)

    # -----------------------------------------------------
    #  事故相關 — 全台事故密度地圖 (Density Map) 
    # -----------------------------------------------------

    st.markdown("""
<hr style="
    border: 0;
    height: 4px;
    background: linear-gradient(90deg, #fdd835, #fff59d);
    border-radius: 2px;
">
""", unsafe_allow_html=True)
    st.markdown(
    f"""
    <h3 style='font-size:25px; font-weight:600; margin-bottom:10px;'>
        🗺️ 全台事故密度地圖（Density Map）
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
    
<<<<<<< HEAD
    # 台灣中心點（台中）
    taiwan_center = [23.7, 120.97]

    m = folium.Map(
        location=taiwan_center,
        zoom_start=7.4,
        tiles="OpenStreetMap"
    )

    # 權重（
    date_filtered_df["heat_weight"] = date_filtered_df.apply(
        lambda r: 5 if r["death_count"] > 0 else (2 if r["injury_count"] > 0 else 1),
        axis=1
    )

    # 熱力圖
    HeatMap(
        date_filtered_df[["latitude", "longitude", "heat_weight"]].values.tolist(),
        radius=18,
        blur=15,
        min_opacity=0.3
    ).add_to(m)

    st_folium(m, width=700, height=450)

=======
    m = build_taiwan_density_map(date_filtered_df)
    st_folium(m, width=700, height=450)


>>>>>>> Tom
    st.markdown(""" *** """)

    # -----------------------------------------------------
    # 城市排行榜（City Ranking）
    # -----------------------------------------------------
    st.subheader("🏙️ 城市排行榜（City Ranking）")

    st.markdown("""
    <hr style="
        border: 0;
        height: 4px;
        background: linear-gradient(90deg, #fdd835, #fff59d);
        border-radius: 2px;
    ">
    """, unsafe_allow_html=True)

    st.markdown("""
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
        【 城市排行榜 】可用於：<br>
        • 找出哪個城市的夜市最危險<br>
        • 哪個城市夜市最多<br>
        • 哪個城市需要優先改善行人安全
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""***""")

    # -----------------------------------------------------
<<<<<<< HEAD
    # 1. 計算每個夜市的 PDI
=======
    # ⭐ 1. 高效能 PDI 計算（向量化 + 空間匹配）
>>>>>>> Tom
    # -----------------------------------------------------
    WEIGHT_DEATH = 5
    WEIGHT_INJURY = 2
    WEIGHT_OPEN = 3
    WEIGHT_CLOSE = 1

<<<<<<< HEAD
    date_filtered_df["accident_datetime"] = pd.to_datetime(date_filtered_df["accident_datetime"])

    pdi_rows = []
    for _, nm in nm_df.iterrows():
        nm_name = nm["nightmarket_name"]

        lat_min = nm["nightmarket_southwest_latitude"]
        lat_max = nm["nightmarket_northeast_latitude"]
        lon_min = nm["nightmarket_southwest_longitude"]
        lon_max = nm["nightmarket_northeast_longitude"]

        acc_in_nm = date_filtered_df[
            (date_filtered_df["latitude"] >= lat_min) &
            (date_filtered_df["latitude"] <= lat_max) &
            (date_filtered_df["longitude"] >= lon_min) &
            (date_filtered_df["longitude"] <= lon_max)
        ]

        total_pdi = 0
        for _, acc in acc_in_nm.iterrows():
            acc_time = acc["accident_datetime"]
            hour = acc_time.hour

            severity = acc["death_count"] * WEIGHT_DEATH + acc["injury_count"] * WEIGHT_INJURY
            is_open = (hour >= 17 or hour == 0)
            weight = WEIGHT_OPEN if is_open else WEIGHT_CLOSE

            total_pdi += severity * weight

        pdi_rows.append({
            "nightmarket_name": nm_name,
            "accident_count": len(acc_in_nm),
            "pdi": total_pdi
        })

    pdi_raw = pd.DataFrame(pdi_rows)

    # 清理字串
    pdi_raw["nightmarket_name"] = pdi_raw["nightmarket_name"].str.strip()
    nm_df["nightmarket_name"] = nm_df["nightmarket_name"].str.strip()

    # 合併城市資訊
=======
    df = date_filtered_df.copy()
    df["accident_datetime"] = pd.to_datetime(df["accident_datetime"])
    df["hour"] = df["accident_datetime"].dt.hour

    df["severity"] = df["death_count"] * WEIGHT_DEATH + df["injury_count"] * WEIGHT_INJURY
    df["is_open"] = df["hour"].between(17, 23)
    df["weight"] = np.where(df["is_open"], WEIGHT_OPEN, WEIGHT_CLOSE)
    df["pdi"] = df["severity"] * df["weight"]

    # ⭐ 空間匹配（一次性）
    def match_nightmarket_fast(df, nm_df):
        nm_df = nm_df.copy()
        nm_df["id"] = nm_df.index

        boxes = nm_df[[
            "nightmarket_southwest_latitude",
            "nightmarket_northeast_latitude",
            "nightmarket_southwest_longitude",
            "nightmarket_northeast_longitude",
        ]].values

        acc_lat = df["latitude"].values
        acc_lon = df["longitude"].values

        match = np.full(len(df), None, dtype=object)

        for idx, (lat_min, lat_max, lon_min, lon_max) in enumerate(boxes):
            inside = (
                (acc_lat >= lat_min) &
                (acc_lat <= lat_max) &
                (acc_lon >= lon_min) &
                (acc_lon <= lon_max)
            )
            match[inside] = nm_df.loc[idx, "nightmarket_name"]

        return match

    df["nightmarket_name"] = match_nightmarket_fast(df, nm_df)

    # ⭐ groupby 計算夜市 PDI
    pdi_raw = (
        df.dropna(subset=["nightmarket_name"])
        .groupby("nightmarket_name")
        .agg(
            accident_count=("accident_id", "count"),
            pdi=("pdi", "sum")
        )
        .reset_index()
    )

    pdi_raw["nightmarket_name"] = pdi_raw["nightmarket_name"].str.strip()
    nm_df["nightmarket_name"] = nm_df["nightmarket_name"].str.strip()

>>>>>>> Tom
    pdi_raw = pdi_raw.merge(
        nm_df[["nightmarket_name", "nightmarket_city"]],
        on="nightmarket_name",
        how="left"
    )

    # -----------------------------------------------------
<<<<<<< HEAD
    # 2. 依照城市分組統計
=======
    # 2. 城市統計（UI 不動）
>>>>>>> Tom
    # -----------------------------------------------------
    city_rank = pdi_raw.groupby("nightmarket_city").agg(
        夜市數量=("nightmarket_name", "count"),
        事故總數=("accident_count", "sum"),
        PDI總和=("pdi", "sum"),
    ).reset_index()

    city_rank = city_rank.rename(columns={"nightmarket_city": "城市"})
<<<<<<< HEAD

    # 平均危險程度
    city_rank["平均PDI"] = (city_rank["PDI總和"] / city_rank["夜市數量"]).round(1)

    # 危險等級
=======
    city_rank["平均PDI"] = (city_rank["PDI總和"] / city_rank["夜市數量"]).round(1)

>>>>>>> Tom
    def danger_level(pdi):
        if pdi <= 10:
            return "🟢 安全"
        elif pdi <= 30:
            return "🟡 注意"
        elif pdi <= 60:
            return "🟠 危險"
        else:
            return "🔴 極危險"

    city_rank["危險等級"] = city_rank["平均PDI"].apply(danger_level)

<<<<<<< HEAD
    # -----------------------------------------------------
    # ⭐ 自動判斷台灣五大區域
    # -----------------------------------------------------
    region_map = {
        "臺北市": "北部", "新北市": "北部", "基隆市": "北部",
        "桃園市": "北部", "新竹市": "北部", "新竹縣": "北部",

        "臺中市": "中部", "苗栗縣": "中部", "彰化縣": "中部",
        "南投縣": "中部", "雲林縣": "中部",

        "臺南市": "南部", "高雄市": "南部", "嘉義市": "南部",
        "嘉義縣": "南部", "屏東縣": "南部",

        "宜蘭縣": "東部", "花蓮縣": "東部", "臺東縣": "東部",

=======
    region_map = {
        "臺北市": "北部", "新北市": "北部", "基隆市": "北部",
        "桃園市": "北部", "新竹市": "北部", "新竹縣": "北部",
        "臺中市": "中部", "苗栗縣": "中部", "彰化縣": "中部",
        "南投縣": "中部", "雲林縣": "中部",
        "臺南市": "南部", "高雄市": "南部", "嘉義市": "南部",
        "嘉義縣": "南部", "屏東縣": "南部",
        "宜蘭縣": "東部", "花蓮縣": "東部", "臺東縣": "東部",
>>>>>>> Tom
        "澎湖縣": "離島", "金門縣": "離島", "連江縣": "離島"
    }

    city_rank["區域"] = city_rank["城市"].map(region_map).fillna("其他")
<<<<<<< HEAD

    # 調整欄位順序
    city_rank = city_rank[["區域", "城市", "夜市數量", "事故總數", "PDI總和", "平均PDI", "危險等級"]]

    # 排序
=======
    city_rank = city_rank[["區域", "城市", "夜市數量", "事故總數", "PDI總和", "平均PDI", "危險等級"]]
>>>>>>> Tom
    city_rank = city_rank.sort_values("PDI總和", ascending=False).reset_index(drop=True)

    st.dataframe(city_rank, hide_index=True, use_container_width=True)

    st.markdown("""***""")

    # -----------------------------------------------------
<<<<<<< HEAD
    # 夜市事故密度與 PDI 排名
    # -----------------------------------------------------

    # 1. 使用者選縣市 → 篩選該縣市的夜市
    city_nightmarkets = nm_df[nm_df["nightmarket_city"] == selected_city]

    # 2. 找出事故屬於哪個夜市（依縣市 + 日期篩選）
    def match_night_market(acc, nightmarkets):
        for _, nm in nightmarkets.iterrows():
            if (
                nm["nightmarket_southwest_latitude"] <= acc["latitude"] <= nm["nightmarket_northeast_latitude"]
                and nm["nightmarket_southwest_longitude"] <= acc["longitude"] <= nm["nightmarket_northeast_longitude"]

            ):
                return nm["nightmarket_name"]
        return None

    date_filtered_df["nightmarket_name"] = date_filtered_df.apply(
        lambda row: match_night_market(row, city_nightmarkets),
        axis=1
    )

    # ⭐ 只保留該縣市的夜市事故
    city_accidents = date_filtered_df.dropna(subset=["nightmarket_name"])

    # 3. PDI 計算
=======
    # 夜市事故密度與 PDI 排名（依縣市）
    # -----------------------------------------------------
    city_nightmarkets = nm_df[nm_df["nightmarket_city"] == selected_city]

    # ⭐ 高效能：使用前面 df 的 nightmarket_name
    city_accidents = df[df["nightmarket_name"].isin(city_nightmarkets["nightmarket_name"])]

>>>>>>> Tom
    pdi_raw = mt.calculate_pdi(city_accidents, nm_df)

    pdi_raw["nightmarket_name"] = pdi_raw["nightmarket_name"].str.strip()
    nm_df["nightmarket_name"] = nm_df["nightmarket_name"].str.strip()

    pdi_raw = pdi_raw.merge(
        nm_df[["nightmarket_name", "nightmarket_id"]],
        on="nightmarket_name",
        how="left"
    )

    pdi_rank = pdi_raw.merge(
        nm_df[["nightmarket_id", "nightmarket_url"]],
        on="nightmarket_id",
        how="left"
    )

<<<<<<< HEAD
    # ⭐ 只保留該縣市的夜市
=======
>>>>>>> Tom
    pdi_rank = pdi_rank[
        pdi_rank["nightmarket_name"].isin(city_nightmarkets["nightmarket_name"].tolist())
    ]

    pdi_rank = pdi_rank.sort_values("pdi", ascending=False).reset_index(drop=True)

<<<<<<< HEAD
    # 4. 呈現表格（用 pdi_rank）
=======
>>>>>>> Tom
    pdi_df = pdi_rank.copy()
    pdi_df["危險等級"] = pdi_df["pdi"].apply(mt.danger_level)

    pdi_df = pdi_df.rename(columns={
        "nightmarket_name": "夜市名稱",
        "accident_count": "事故數",
        "pdi": "PDI",
    })

    pdi_df = pdi_df[["夜市名稱", "事故數", "PDI", "危險等級"]]

    st.subheader("📈 夜市事故密度與 PDI 排名（依縣市）")
    st.dataframe(pdi_df, hide_index=True)

    st.markdown("""
    <style>
    .yellow-text {
<<<<<<< HEAD
        color: #FFD700;          /* 黃色 */
        font-weight: bold;       /* 粗體 */
        font-size: 20px;         /* 字體大小：你可以改成 18px、22px 等 */
=======
        color: #FFD700;
        font-weight: bold;
        font-size: 20px;
>>>>>>> Tom
    }
    </style>

    <div class="yellow-text">
    <hr>
    <ul>
    <li>事故嚴重度 = 死亡 × 5 + 受傷 × 2</li>
    <li>時段權重 = 3（營業時段）/ 1（非營業時段）</li>
    </ul>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""***""")
<<<<<<< HEAD
    
    # -----------------------------------------------------
    # 4. 高風險時段（使用 Altair）
    # -----------------------------------------------------

=======

    # -----------------------------------------------------
    # 4. 高風險時段（使用 Altair）
    # -----------------------------------------------------
>>>>>>> Tom
    time_risk = None
    weather_risk = None
    light_risk = None
    pdi_df = None
<<<<<<< HEAD
    city_rank = None
    
    st.subheader(f"📊 {selected_city}高風險時段分析")
=======

    st.subheader(f"📊 {selected_city} 高風險時段分析")
>>>>>>> Tom

    if city_accidents.empty:
        st.info("⚠️ 此縣市在此期間沒有夜市事故，因此無法進行時段分析。")
    else:
<<<<<<< HEAD
        city_accidents["accident_hour"] = city_accidents["accident_hour"].astype(int)
=======
        city_accidents = city_accidents.copy()
        city_accidents["accident_hour"] = city_accidents["accident_datetime"].dt.hour
>>>>>>> Tom

        time_risk = (
            city_accidents.groupby("accident_hour")
            .size()
            .reset_index(name="事故數")
            .sort_values("accident_hour")
        )

        chart = (
            alt.Chart(time_risk)
            .mark_bar(color="#4BA3FF")
            .encode(
                x=alt.X("accident_hour:O", title="時段（小時）", sort="ascending", axis=alt.Axis(labelAngle=0)),
                y=alt.Y("事故數:Q", title="事故數"),
                tooltip=["accident_hour", "事故數"]
            )
            .properties(width=600, height=350)
        )

        st.altair_chart(chart, use_container_width=True)

    # -----------------------------------------------------
    # 5. 天氣風險
    # -----------------------------------------------------
    st.subheader(f"🔍 {selected_city} 天氣風險分析")

    if city_accidents.empty:
        st.info("⚠️ 沒有事故資料，無法進行天氣風險分析。")
    else:
<<<<<<< HEAD
        city_acc_ids = city_accidents["accident_id"].tolist()

        city_weather = date_filtered_df[
            date_filtered_df["accident_id"].isin(city_acc_ids)
        ]
=======
        city_weather = df[df["accident_id"].isin(city_accidents["accident_id"])]
>>>>>>> Tom

        if city_weather.empty:
            st.info("⚠️ 此縣市在此期間沒有天氣相關事故資料。")
        else:
            weather_risk = (
                city_weather.groupby("weather_condition")
                .size()
                .reset_index(name="事故數")
            )

            weather_risk["比例 (%)"] = (
                weather_risk["事故數"] / weather_risk["事故數"].sum() * 100
            ).round(1)

            weather_risk = weather_risk.rename(columns={
                "weather_condition": "天氣狀況"
            })

            st.dataframe(weather_risk, hide_index=True)

    # -----------------------------------------------------
    # 6. 道路環境風險
    # -----------------------------------------------------
    st.subheader(f"🔍 {selected_city} 道路環境風險分析")

    if city_accidents.empty:
        st.info("⚠️ 沒有事故資料，無法進行道路環境風險分析。")
    elif "light_condition" not in city_accidents.columns:
        st.warning("⚠️ 資料中沒有光線狀況（light_condition）欄位，無法分析道路環境風險。")
    else:
        light_risk = (
            city_accidents.groupby("light_condition")
            .size()
            .reset_index(name="事故數")
        )

        light_risk["比例 (%)"] = (
            light_risk["事故數"] / light_risk["事故數"].sum() * 100
        ).round(1)

        light_risk = light_risk.rename(columns={
            "light_condition": "光線狀況"
        })

        st.dataframe(light_risk, hide_index=True)

<<<<<<< HEAD
=======


>>>>>>> Tom
    # ============================
    # ⭐ 第四章 AI 所需資訊 (自動化 summary_text)
    # ============================

    # ⭐ 產生 V4 5句洞察（避免 city_rank 是 None）
    if city_rank is None or city_rank.empty:
        insight_text = "目前沒有足夠資料產生洞察。"
    else:
        insight_text = mt.generate_insight_V4(city_rank)

    # ⭐ 產生 5 句模板
    templates = [insight_text] * 5

    # ⭐ 把 CH4 資料放入 session_state
    st.session_state["ch4"] = {
        "selected_city": selected_city,
        "date_filtered_df": date_filtered_df,
        "city_rank": city_rank,
        "pdi_df": pdi_df,
        "time_risk": time_risk,
        "weather_risk": weather_risk,
        "light_risk": light_risk,
        "templates": templates
    }
