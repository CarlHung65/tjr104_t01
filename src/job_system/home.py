from utils.sidebar import sidebar_filters
from streamlit_folium import st_folium
from data_loader import load_night_markets, load_accidents
from utils.maps_tool import build_map
import streamlit as st
import pandas as pd
import calendar
import random
import utils.market_tools as mt

# -----------------------------------------------------
# nm_df (夜市資料) - DataFrame
# -----------------------------------------------------
nm_df = load_night_markets() 

# ---------------------------------------------------------
# 主頁面
# ---------------------------------------------------------
def render_home():

    if st.session_state.page != "夜市行人地獄(?)":
        return

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
    # ⭐ 取得 sidebar 的篩選結果（不再回傳 date_filtered_df）
    # -----------------------------------------------------
    selected_city, selected_market, date_filter_type, date_info = sidebar_filters(
        load_city_list,
        load_market_by_city,
    )

    # -----------------------------------------------------
    # ⭐ 在這裡才查資料（每頁只查一次）
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
    <div style="
        font-size: 20px;
        font-weight: 800;
        color: #FFD54F;
        margin-bottom: 6px;
    ">
        選擇事故熱力圖類型
    </div>
    """, unsafe_allow_html=True)

    heat_mode = st.selectbox("", ["☀️ 白天事故熱力圖", "🌙 夜間事故熱力圖"])

    # 中心點
    current_nm = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]
    center_lat = current_nm["nightmarket_latitude"]
    center_lon = current_nm["nightmarket_longitude"]

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

    # ⭐ 決定 map_type 與 plot_df
    if heat_mode.startswith("☀️"):
        map_type = "day_heat"
        plot_df = day_df
    else:
        map_type = "night_heat"
        plot_df = night_df

    # ⭐ 呼叫 build_map（只要這一行）
    m = build_map(
        map_type=map_type,
        nm_df=nm_df,
        center_lat=center_lat,
        center_lon=center_lon,
        plot_df=plot_df
    )

    # ⭐ 顯示地圖
    st_folium(m, width=700, height=450)

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

    # ⭐ 表格資料（保留你的欄位名稱）
    pdi_df = pdi_rank.copy()
    pdi_df["危險等級"] = pdi_df["pdi"].apply(mt.danger_level)
    pdi_df = pdi_df.rename(columns={
        "nightmarket_name": "夜市名稱",
        "accident_count": "事故數",
        "pdi": "PDI",
    })
    pdi_df = pdi_df[["夜市名稱", "事故數", "PDI", "危險等級"]]

    st.dataframe(pdi_df, hide_index=True)

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

    # ⭐ 計算每筆事故的 PDI 權重
    nm_acc = calculate_pdi_points(date_filtered_df, nm_row)

    # ⭐ HeatMap 需要的格式
    pdi_points = nm_acc[["latitude", "longitude", "pdi_weight"]].values.tolist()

    # ⭐ 夜市框線
    bounds = (
        nm_row["nightmarket_southwest_latitude"],
        nm_row["nightmarket_southwest_longitude"],
        nm_row["nightmarket_northeast_latitude"],
        nm_row["nightmarket_northeast_longitude"]
    )

    # ⭐ 呼叫 build_map()
    pdi_map = build_map(
        map_type="pdi_heat",
        nm_df=nm_df,
        center_lat=center_lat,
        center_lon=center_lon,
        extra={
            "pdi_points": pdi_points,
            "bounds": bounds
        }
    )

    st_folium(pdi_map, width=700, height=450)


    # -----------------------------------------------------
    # ⭐ 夜市評分 vs 危險程度
    # -----------------------------------------------------
    st.subheader("⭐ 夜市評分  v s  危險程度")

    rating_df = (
        pdi_rank
        .merge(
            nm_df[["nightmarket_id", "nightmarket_rating"]],
            on="nightmarket_id",
            how="left"
        )
        .assign(危險等級=lambda df: df["pdi"].apply(mt.danger_level))
        .sort_values(["nightmarket_rating", "pdi"], ascending=[False, False])
        [["nightmarket_name", "nightmarket_rating", "pdi", "危險等級"]]
        .rename(columns={
            "nightmarket_name": "夜市名稱",
            "nightmarket_rating": "Google 評分",
            "pdi": "PDI"
        })
    )

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

    # ⭐ 從完整 pdi_rank 取前 4 名
    top4 = pdi_rank.sort_values("pdi", ascending=False).head(4)

    html_template = """
<div class="pdi-card" style="background:{bg}; padding:16px; border-radius:12px; box-shadow:0 4px 10px rgba(0,0,0,0.15); transition:0.2s;">
<a href="{url}" target="_blank" style="text-decoration:none;color:inherit;">
<div style="font-size:20px;font-weight:bold;margin-bottom:8px;color:white;text-shadow:0px 1px 2px rgba(0,0,0,0.28);">
    {icon} 第 {rank} 名</div>
<div style="font-size:18px;font-weight:bold;margin-bottom:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:white;
    padding-bottom:3px;
    border-bottom:3px solid rgba(255,255,255,0.55);
    text-shadow:0px 1px 2px rgba(0,0,0,0.28);
    ">{name}({city})</div>
<div style="font-size:16px;margin-bottom:4px;color:white;text-shadow:0px 1px 2px rgba(0,0,0,0.28);">
    PDI：<b>{pdi}</b>（{level}）
    </div>
<div style="font-size:14px;color:#333;">
    事故數：{count} 件
</div>
    </a>
</div>
    """

    rank_icons = ["🥇", "🥈", "🥉", ""]

    cards_html = '<div class="pdi-card-container">'
    for i, (_, row) in enumerate(top4.iterrows()):
        cards_html += html_template.format(
            rank=i + 1,
            icon=rank_icons[i],
            name=row['nightmarket_name'],
            city=row['nightmarket_city'],
            pdi=row['pdi'],
            level=mt.danger_level(row["pdi"]),
            count=row['accident_count'],
            url=row['nightmarket_url'],
            bg=mt.danger_color(row["pdi"])
        )
    cards_html += '</div>'

    st.markdown(cards_html, unsafe_allow_html=True)
    st.markdown(""" *** """)

    # -----------------------------------------------------
    # ⭐ 事故數排行榜（Accident Count Top 4）
    # -----------------------------------------------------
    st.subheader("🚨 事故數排行榜 Top 4")

    accident_top4 = pdi_rank.sort_values("accident_count", ascending=False).head(4)

    cards_html = '<div class="pdi-card-container">'
    for i, (_, row) in enumerate(accident_top4.iterrows()):
        cards_html += html_template.format(
            rank=i + 1,
            icon=rank_icons[i],
            name=row['nightmarket_name'],
            city=row['nightmarket_city'],
            pdi=row['pdi'],
            level=mt.danger_level(row["pdi"]),
            count=row['accident_count'],
            url=row['nightmarket_url'],
            bg=mt.danger_color(row["pdi"])
        )
    cards_html += '</div>'

    st.markdown(cards_html, unsafe_allow_html=True)
    mt.pdi_divider(last_level)

    # -----------------------------------------------------
    # ⭐ 第一章 AI 所需資訊 (自動化 summary_text)
    # -----------------------------------------------------

    # ⭐ PDI Top4（從完整 pdi_rank 取前 4 名）
    pdi_top4 = pdi_rank.sort_values("pdi", ascending=False).head(4)

    # ⭐ 存入 session_state
    st.session_state["ch1"] = {
        "selected_market": selected_market,
        "date_filtered_df": date_filtered_df,
        "day_df": day_df,
        "night_df": night_df,
        "pdi_df": pdi_df,
        "nm_acc": nm_acc,
        "rating_df": rating_df,
        "pdi_top4": pdi_top4,         
        "accident_top4": accident_top4 
    }

