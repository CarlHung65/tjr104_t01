from utils.sidebar import sidebar_filters
from folium.plugins import HeatMap
from streamlit_folium import st_folium
from data_loader import load_night_markets, load_accidents
import streamlit as st
import pandas as pd
import folium
import datetime
import re
import random
import utils.market_tools as mt

# -----------------------------------------------------
# nm_df (夜市資料) - DataFrame
# -----------------------------------------------------
nm_df = load_night_markets() 

# -----------------------------------------------------
# accidents_df (事故資料) - DataFrame
# -----------------------------------------------------
accidents_df = load_accidents(columns="accident_id, latitude, longitude, death_count, injury_count, accident_datetime")

accidents_df["accident_datetime"] = pd.to_datetime(accidents_df["accident_datetime"])
accidents_df["accident_weekday"] = accidents_df["accident_datetime"].dt.weekday
accidents_df["accident_hour"] = accidents_df["accident_datetime"].dt.hour

# ---------------------------------------------------------
# 主頁面
# ---------------------------------------------------------
def render_home():

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
    # V1 洞察（generate_insight）
    # -----------------------------------------------------
    # 產生洞察
    insight_text = mt.generate_insight_V1(date_filtered_df, nm_df, selected_market)

    # 標題 + 洞察框（右上角）
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


    st.markdown("""
    <div style="height: 40px;"></div>
    """, unsafe_allow_html=True)
    # -----------------------------------------------------
    # 正文開始
    # -----------------------------------------------------   
    st.markdown("""
<h2>
夜市行人<b style="color:red;">地獄</b>(❓)：<span style="margin-right:10px;">數據</span>揭露的真相
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
第一步：<b style="color:#f7c843;">Why</b> — 夜市真的很安全嗎？ <span style="margin-right:10px;">數據</span>怎麼說？
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
    外媒直指臺灣街頭是<b style="color:red;">「行人地獄」</b>，交通事故死亡人數驚人。城市規劃偏車不偏人、駕駛陋習成常態，讓過馬路像賭命！
<div style="height: 15px;"></div>
<div style="font-size: 24px;">
  🚦 <span style="margin-right:10px;">為什麼 — </span>我們<b style="color:#1e90ff;"> 關注 </b>夜市安全？
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
    我們用數據工程打造互動地圖：解析肇因比例、即時告警、用路人評論，
    甚至串接天氣 / 人流 / 節慶，揭露雨夜塞車、寒流車速亂飆的隱藏殺機。
</div>
</div>
    """, unsafe_allow_html=True)

    # -----------------------------------------------------
    # 日夜熱力圖（圓形 + 標籤 + 事故點）
    # -----------------------------------------------------
    st.markdown(""" *** """)
    st.subheader(f"📍 {selected_market} — 事故熱點地圖（可切換圖層）")
    st.markdown("""
    <hr style="
        border: 0;
        height: 4px;
        background: linear-gradient(90deg, #fdd835, #fff59d);
        border-radius: 2px;
    ">
    """, unsafe_allow_html=True)

    # 自訂標題（可完全控制字體大小）
    st.markdown("""
    <div style="
        font-size: 20px;
        font-weight: 800;
        color: #FFD54F;
        margin-bottom: 6px;
    ">
        選擇事故熱力圖類型
    </div>
    """, unsafe_allow_html=True)

    # selectbox 本身不顯示標題
    heat_mode = st.selectbox(
        "",
        ["☀️ 白天事故熱力圖", "🌙 夜間事故熱力圖"]
    )

    # 中心點
    current_nm = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]
    center_lat = current_nm["nightmarket_latitude"]
    center_lon = current_nm["nightmarket_longitude"]

    # 建立地圖
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=16,
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
            fill_opacity=0.35
        ).add_to(m)

        folium.Marker(
            location=[
                row["nightmarket_latitude"] + 0.0006,
                row["nightmarket_longitude"] - 0.0006
            ],
            icon=folium.DivIcon(
                html=f"""
    <div style="
        font-size:16px;
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

    # 事故點權重
    date_filtered_df["heat_weight"] = date_filtered_df.apply(
        lambda r: 5 if r["death_count"] > 0 else (2 if r["injury_count"] > 0 else 1),
        axis=1
    )

    # 白天 / 夜間資料切割
    date_filtered_df["acc_time"] = pd.to_datetime(date_filtered_df["accident_datetime"])
    date_filtered_df["hour"] = date_filtered_df["acc_time"].dt.hour

    day_df = date_filtered_df[(date_filtered_df["hour"] >= 6) & (date_filtered_df["hour"] < 18)]
    night_df = date_filtered_df[(date_filtered_df["hour"] >= 18) | (date_filtered_df["hour"] < 6)]

    # ⭐ 根據選擇決定要畫哪一組資料（事故點 + 熱力圖都用這組）
    if heat_mode == "☀️ 白天事故熱力圖":
        plot_df = day_df
        layer = folium.FeatureGroup(name="☀️ 白天事故熱力圖")
    else:
        plot_df = night_df
        layer = folium.FeatureGroup(name="🌙 夜間事故熱力圖")

    # ⭐ 熱力圖
    HeatMap(
        plot_df[["latitude", "longitude", "heat_weight"]].values.tolist(),
        radius=18,
        blur=15,
        min_opacity=0.3
    ).add_to(layer)

    layer.add_to(m)

    # ⭐ 事故點（小圓點）
    for _, row in plot_df.iterrows():    

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

  
   # 顯示地圖
    st_folium(m, width=700, height=450, returned_objects=[])

    # -----------------------------------------------------
    # 區塊 2：PDI 說明
    # -----------------------------------------------------
    st.markdown("""
    <div style="
        width: 100%;
        height: 0.5px;
        background: linear-gradient(90deg, white, rgba(255,255,255,0.2));
        border-radius: 3px;
    "></div>
    """, unsafe_allow_html=True)

    st.subheader("⚠️ 夜市危險指數（PDI - Pedestrian Danger Index）")

    st.markdown("""
    <hr style="
        border: 0;
        height: 4px;
        background: linear-gradient(90deg, #fdd835, #fff59d);
        border-radius: 2px;
    ">
    """, unsafe_allow_html=True)

    st.markdown("""
    | 0–10 🟢 安全 | 11–30 🟡 注意 | 31–60 🟠 危險 |  >60 🔴 極危險 | ⚠️ PDI = Σ（事故嚴重度 × 時段權重）
    """)


    # -----------------------------------------------------
    # 1. 計算 PDI（使用工具版）
    # -----------------------------------------------------
    pdi_raw = mt.calculate_pdi(date_filtered_df, nm_df)

    # 清理字串
    pdi_raw["nightmarket_name"] = pdi_raw["nightmarket_name"].str.strip()
    nm_df["nightmarket_name"] = nm_df["nightmarket_name"].str.strip()

    # ⭐ 帶入 nightmarket_id + nightmarket_city
    pdi_raw = pdi_raw.merge(
        nm_df[["nightmarket_name", "nightmarket_id", "nightmarket_city"]],
        on="nightmarket_name",
        how="left"
    )

    # 找回 nightmarket_url
    pdi_rank = pdi_raw.merge(
        nm_df[["nightmarket_id", "nightmarket_url"]],
        on="nightmarket_id",
        how="left"
    )

    # ⭐ 依縣市篩選（使用 selected_city）
    pdi_rank = pdi_rank[pdi_rank["nightmarket_city"] == selected_city]

    # 排序
    pdi_rank = pdi_rank.sort_values("pdi", ascending=False).reset_index(drop=True)

    # 表格資料
    pdi_df = pdi_rank.copy()
    pdi_df["危險等級"] = pdi_df["pdi"].apply(mt.danger_level)
    pdi_df = pdi_df.rename(columns={
        "nightmarket_name": "夜市名稱",
        "accident_count": "事故數",
        "pdi": "PDI",
    })
    pdi_df = pdi_df[["夜市名稱", "事故數", "PDI", "危險等級"]]

    st.dataframe(pdi_df, hide_index=True)

    st.markdown("""
    <style>
    .yellow-text {
        color: #FFD700;          /* 黃色 */
        font-weight: bold;       /* 粗體 */
        font-size: 20px;         /* 字體大小：你可以改成 18px、22px 等 */
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

    st.markdown(""" *** """)
                
    st.markdown("""
    <div style="
        width: 100%;
        height: 0.5px;
        background: linear-gradient(90deg, white, rgba(255,255,255,0.2));
        border-radius: 3px;
    "></div>
    """, unsafe_allow_html=True)

    # -----------------------------------------------------
    # ⭐ PDI 熱力圖（依工具版 PDI 計算）
    # -----------------------------------------------------

    st.markdown(
        f"""
        <h3 style='font-size:25px; font-weight:600; margin-bottom:10px;'>
            🔥 {selected_market} ｜ PDI 事故熱力圖
        </h3>
        """,
        unsafe_allow_html=True
    )

    # 找到該夜市資料
    nm_row = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]

    center_lat = nm_row["nightmarket_latitude"]
    center_lon = nm_row["nightmarket_longitude"]


    # ⭐ 使用工具版邏輯計算每筆事故的 PDI 權重
    def calculate_pdi_points(acc_df, nm_row):
        WEIGHT_DEATH = 5
        WEIGHT_INJURY = 2
        WEIGHT_OPEN = 3
        WEIGHT_CLOSE = 1

        bbox = mt.get_bbox(nm_row)
        opening_hours = mt.parse_opening_hours(nm_row["nightmarket_opening_hours"])

        acc_in_nm = acc_df[
            acc_df.apply(
                lambda row: mt.accident_in_bbox(row["latitude"], row["longitude"], bbox),
                axis=1
            )
        ].copy()

        # ⭐ 防呆：如果沒有資料或沒有欄位，直接回傳空 DF
        if acc_in_nm.empty or acc_in_nm.shape[1] == 0:
            return pd.DataFrame(columns=["latitude", "longitude", "pdi_weight"])

        acc_in_nm["pdi_weight"] = acc_in_nm.apply(
            lambda row: (
                row["death_count"] * WEIGHT_DEATH +
                row["injury_count"] * WEIGHT_INJURY
            ) * (
                WEIGHT_OPEN if mt.is_in_opening(row["accident_datetime"], opening_hours)
                else WEIGHT_CLOSE
            ),
            axis=1
        )

        return acc_in_nm


    nm_acc = calculate_pdi_points(date_filtered_df, nm_row)

    # folium 地圖
    pdi_map = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=17,
        tiles="OpenStreetMap"
    )

    # ⭐ HeatMap（使用工具版 PDI 權重）
    heat_points = nm_acc[["latitude", "longitude", "pdi_weight"]].values.tolist()

    HeatMap(
        heat_points,
        radius=18,
        max_zoom=15,
        blur=20,
        max_val=nm_acc["pdi_weight"].max()
    ).add_to(pdi_map)

    # 夜市範圍框
    folium.Rectangle(
        bounds=[
            [nm_row["nightmarket_southwest_latitude"], nm_row["nightmarket_southwest_longitude"]],
            [nm_row["nightmarket_northeast_latitude"], nm_row["nightmarket_northeast_longitude"]]
        ],
        color="#FF8C42",
        weight=2,
        fill=True,
        fill_opacity=0.15
    ).add_to(pdi_map)

    # ⭐⭐⭐ 用 st_folium 固定大小（最穩定）
    st_folium(
        pdi_map,
        width=700,
        height=450,
        returned_objects=[]
    )

    # -----------------------------------------------------
    # ⭐ 夜市評分 vs 危險程度
    # -----------------------------------------------------
    st.subheader("⭐ 夜市評分  v s  危險程度")

    rating_merge = pdi_rank.merge(
        nm_df[["nightmarket_id", "nightmarket_rating"]],
        on="nightmarket_id",
        how="left"
    )

    rating_merge = rating_merge.sort_values(
        ["nightmarket_rating", "pdi"], ascending=[False, False]
    )

    rating_merge["危險等級"] = rating_merge["pdi"].apply(mt.danger_level)

    rating_df = rating_merge[[
        "nightmarket_name",
        "nightmarket_rating",
        "pdi",
        "危險等級"
    ]].rename(columns={
        "nightmarket_name": "夜市名稱",
        "nightmarket_rating": "Google 評分",
        "pdi": "PDI"
    })

    st.dataframe(rating_df, hide_index=True)

    top_level = mt.danger_level(pdi_rank.iloc[0]["pdi"]).replace("🟢 ", "").replace("🟡 ", "").replace("🟠 ", "").replace("🔴 ", "")
    last_level = mt.danger_level(pdi_rank.iloc[-1]["pdi"]).replace("🟢 ", "").replace("🟡 ", "").replace("🟠 ", "").replace("🔴 ", "")
    mt.pdi_divider(top_level)

    # -----------------------------------------------------
    # ⭐ 夜市危險指數 Top 4
    # -----------------------------------------------------
    st.subheader("🔥 夜市危險指數 Top 4")

    st.markdown("""
    <style>
    .pdi-card-container {
        display: flex;
        flex-wrap: wrap;
        justify-content: center;
        gap: 16px;
    }
    .pdi-card-container > div.stMarkdown {
        display: flex;
        flex: 0 0 calc(33.33% - 16px);
        max-width: 260px;
        min-width: 200px;
    }
    .pdi-card:hover {
        transform: scale(1.03);
        box-shadow: 0 6px 14px rgba(0,0,0,0.25);
    }
    </style>
    """, unsafe_allow_html=True)

    pdi_raw = mt.calculate_pdi(date_filtered_df, nm_df)
    pdi_raw["nightmarket_name"] = pdi_raw["nightmarket_name"].str.strip()
    nm_df["nightmarket_name"] = nm_df["nightmarket_name"].str.strip()

    pdi_raw = pdi_raw.merge(
        nm_df[["nightmarket_name", "nightmarket_id","nightmarket_city"]],
        on="nightmarket_name",
        how="left"
    )

    pdi_rank = pdi_raw.merge(
        nm_df[["nightmarket_id", "nightmarket_url"]],
        on="nightmarket_id",
        how="left"
    )

    pdi_rank = pdi_rank.sort_values("pdi", ascending=False).head(4).reset_index(drop=True)

    html_template = """
<div class="pdi-card" style="background:{bg}; padding:16px; border-radius:12px; box-shadow:0 4px 10px rgba(0,0,0,0.15); transition:0.2s;">
<a href="{url}" target="_blank" style="text-decoration:none;color:inherit;">
<div style="font-size:20px;font-weight:bold;margin-bottom:8px;color:white;text-shadow:0px 1px 2px rgba(0,0,0,0.28);
">{icon} 第 {rank} 名</div>
<div style="font-size:18px;font-weight:bold;margin-bottom:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:white;
padding-bottom:3px;
border-bottom:3px solid rgba(255,255,255,0.55);
text-shadow:0px 1px 2px rgba(0,0,0,0.28);
">{name}({city})</div>
<div style="font-size:16px;margin-bottom:4px;
color:white;
text-shadow:0px 1px 2px rgba(0,0,0,0.28);
">PDI：<b>{pdi}</b>（{level}）</div>
<div style="font-size:14px;color:#333;">
事故數：{count} 件
</div>
</a>
</div>
"""

    rank_icons = ["🥇", "🥈", "🥉", ""]

    cards_html = '<div class="pdi-card-container">'
    for i, (_, row) in enumerate(pdi_rank.iterrows()):
        bg = mt.danger_color(row["pdi"])
        icon = rank_icons[i]

        cards_html += html_template.format(
            rank=i + 1,
            icon=icon,
            name=row['nightmarket_name'],
            city=row['nightmarket_city'],
            pdi=row['pdi'],
            level=mt.danger_level(row["pdi"]),
            count=row['accident_count'],
            url=row['nightmarket_url'],
            bg=bg
        )
    cards_html += '</div>'

    st.markdown(cards_html, unsafe_allow_html=True)
    st.markdown(""" *** """)

    # -----------------------------------------------------
    # ⭐ 事故數排行榜（Accident Count Top 4）
    # -----------------------------------------------------
    st.subheader("🚨 事故數排行榜 Top 4")

    accident_rank = pdi_rank.sort_values("accident_count", ascending=False).head(4)

    cards_html = '<div class="pdi-card-container">'
    for i, (_, row) in enumerate(accident_rank.iterrows()):
        bg = mt.danger_color(row["pdi"])
        icon = rank_icons[i]

        cards_html += html_template.format(
            rank=i + 1,
            icon=icon,
            name=row['nightmarket_name'],
            city=row['nightmarket_city'],
            pdi=row['pdi'],
            level=mt.danger_level(row["pdi"]),
            count=row['accident_count'],
            url=row['nightmarket_url'],
            bg=bg
        )
    cards_html += '</div>'

    st.markdown(cards_html, unsafe_allow_html=True)
    mt.pdi_divider(last_level)


    # -----------------------------------------------------
    # ⭐ 第一章 AI 所需資訊 (自動化 summary_text)
    # -----------------------------------------------------

    #  把CH1資料放入 session_state

    st.session_state["ch1"] = {
        "selected_market": selected_market,
        "date_filtered_df": date_filtered_df,
        "day_df": day_df,
        "night_df": night_df,
        "pdi_df": pdi_df,
        "nm_acc": nm_acc,
        "rating_df": rating_df,
        "pdi_top4": pdi_rank,
        "accident_top4": accident_rank
    }
