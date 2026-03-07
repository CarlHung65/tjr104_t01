import plotly.express as px
import pandas as pd
import streamlit as st


    # -----------------------------------------------------
    # ⭐ Act 2
    # -----------------------------------------------------
    # -----------------------------------------------------
    # 事故原因分析（玫瑰圖/長條圖）
    # -----------------------------------------------------

def build_cause_charts(selected_market, date_filtered_df, nm_df, human_factors_df):
    # 夜市範圍
    nm = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]

    lat_min = nm["nightmarket_southwest_latitude"]
    lat_max = nm["nightmarket_northeast_latitude"]
    lon_min = nm["nightmarket_southwest_longitude"]
    lon_max = nm["nightmarket_northeast_longitude"]

    # 1. 夜市範圍事故
    accidents_in_nm = date_filtered_df[
        (date_filtered_df["latitude"] >= lat_min) &
        (date_filtered_df["latitude"] <= lat_max) &
        (date_filtered_df["longitude"] >= lon_min) &
        (date_filtered_df["longitude"] <= lon_max)
    ].copy()

    # 2. 人為因素
    human_in_nm = human_factors_df[
        human_factors_df["accident_id"].isin(accidents_in_nm["accident_id"])
    ].copy()

    st.markdown(
        f"""
        <h3 style='font-size:25px; font-weight:600; margin-bottom:10px;'>
            📍 {selected_market} ｜ 事故原因分析（僅夜市範圍）
        </h3>
        """,
        unsafe_allow_html=True
    )

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

    chart_type = st.selectbox("", ["玫瑰圖（Rose Chart）", "水平長條圖"])

    if human_in_nm.empty:
        st.info(f"{selected_market} 範圍內目前沒有事故資料。")
        st.markdown(""" *** """)
        return

    # 3. 統計事故原因
    reason_df = (
        human_in_nm["cause_analysis_minor_primary"]
        .value_counts()
        .reset_index()
    )
    reason_df.columns = ["reason", "count"]

    color_map = [
        "#FF6B6B", "#FFD93D", "#6BCB77",
        "#4D96FF", "#9D4EDD", "#FF8E3C"
    ]

    # 4. 玫瑰圖
    if chart_type == "玫瑰圖（Rose Chart）":
        fig = px.bar_polar(
            reason_df,
            r="count",
            theta="reason",
            color="reason",
            color_discrete_sequence=color_map,
            title=f"{selected_market} ｜ 事故原因玫瑰圖"
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # 5. 水平長條圖
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
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown(""" *** """)

    # 6. 天氣因素
    st.subheader(" 天氣相關因素 ")
    st.markdown("""
    <hr style="
        border: 0;
        height: 4px;
        background: linear-gradient(90deg, #fdd835, #fff59d);
        border-radius: 2px;
    ">
    """, unsafe_allow_html=True)


def build_rain_comparison_charts(selected_market, date_filtered_df, nm_df):
    st.subheader(f"{selected_market}｜雨天 vs 非雨天事故比較（夜市營業時段）")

    mode = st.selectbox("", ["事故量比較", "事故嚴重度比較"])

    # 取得夜市 bounding box
    nm_row = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]
    ne_lat = float(nm_row["nightmarket_northeast_latitude"])
    ne_lon = float(nm_row["nightmarket_northeast_longitude"])
    sw_lat = float(nm_row["nightmarket_southwest_latitude"])
    sw_lon = float(nm_row["nightmarket_southwest_longitude"])

    # 1. 夜市範圍事故
    market_accidents = date_filtered_df[
        (date_filtered_df["latitude"] >= sw_lat) &
        (date_filtered_df["latitude"] <= ne_lat) &
        (date_filtered_df["longitude"] >= sw_lon) &
        (date_filtered_df["longitude"] <= ne_lon)
    ].copy()

    if market_accidents.empty:
        st.warning(f"{selected_market} 範圍內沒有事故資料。")
        st.markdown(""" *** """)
        return

    # 2. 夜市營業時段
    market_accidents["accident_datetime"] = pd.to_datetime(
        market_accidents["accident_datetime"], errors="coerce"
    )
    market_accidents["hour"] = market_accidents["accident_datetime"].dt.hour

    night_df = market_accidents[
        (market_accidents["hour"] >= 17) &
        (market_accidents["hour"] <= 23)
    ].copy()

    if night_df.empty:
        st.warning(f"{selected_market} 在營業時段（17–23）沒有事故資料。")
        st.markdown(""" *** """)
        return
    
    # -----------------------------------------------------
    # 雨天 vs 非雨天事故比較
    # -----------------------------------------------------

def build_rain_comparison_charts(selected_market, date_filtered_df, nm_df):
    st.subheader(f"{selected_market}｜雨天 vs 非雨天事故比較（夜市營業時段）")

    mode = st.selectbox("", ["事故量比較", "事故嚴重度比較"])

    # 取得夜市 bounding box
    nm_row = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]
    ne_lat = float(nm_row["nightmarket_northeast_latitude"])
    ne_lon = float(nm_row["nightmarket_northeast_longitude"])
    sw_lat = float(nm_row["nightmarket_southwest_latitude"])
    sw_lon = float(nm_row["nightmarket_southwest_longitude"])

    # 1. 夜市範圍事故
    market_accidents = date_filtered_df[
        (date_filtered_df["latitude"] >= sw_lat) &
        (date_filtered_df["latitude"] <= ne_lat) &
        (date_filtered_df["longitude"] >= sw_lon) &
        (date_filtered_df["longitude"] <= ne_lon)
    ].copy()

    if market_accidents.empty:
        st.warning(f"{selected_market} 範圍內沒有事故資料。")
        st.markdown(""" *** """)
        return

    # 2. 夜市營業時段
    market_accidents["accident_datetime"] = pd.to_datetime(
        market_accidents["accident_datetime"], errors="coerce"
    )
    market_accidents["hour"] = market_accidents["accident_datetime"].dt.hour

    night_df = market_accidents[
        (market_accidents["hour"] >= 17) &
        (market_accidents["hour"] <= 23)
    ].copy()

    if night_df.empty:
        st.warning(f"{selected_market} 在營業時段（17–23）沒有事故資料。")
        st.markdown(""" *** """)
        return

    # 3. 雨天 vs 非雨天
    night_df["rain_group"] = night_df["weather_condition"].apply(
        lambda w: "雨天" if isinstance(w, str) and "雨" in w else "非雨天"
    )

    # 4. 嚴重度
    night_df["avg_severity"] = (
        night_df["death_count"] * 3 + night_df["injury_count"]
    )

    # 5. 畫圖
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
    # 夜市營業時段｜天氣風險雷達圖（最佳化版本）
    # -----------------------------------------------------

def build_weather_radar_chart(selected_market, date_filtered_df, nm_df, road_factors_df):
    st.subheader(f"🧭 {selected_market}｜夜市營業時段天氣風險雷達圖")

    # 夜市資料
    nm = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]
    lat_min, lat_max = nm["nightmarket_southwest_latitude"], nm["nightmarket_northeast_latitude"]
    lon_min, lon_max = nm["nightmarket_southwest_longitude"], nm["nightmarket_northeast_longitude"]

    # 1. 夜市範圍事故
    acc_nm = date_filtered_df[
        (date_filtered_df["latitude"] >= lat_min) &
        (date_filtered_df["latitude"] <= lat_max) &
        (date_filtered_df["longitude"] >= lon_min) &
        (date_filtered_df["longitude"] <= lon_max)
    ].copy()

    if acc_nm.empty:
        st.info(f"{selected_market} 在營業時段沒有事故資料。")
        return

    # 2. 合併道路資料
    acc_nm["accident_id"] = acc_nm["accident_id"].astype(int)
    road_factors_df["accident_id"] = road_factors_df["accident_id"].astype(int)
    df = acc_nm.merge(road_factors_df, on="accident_id", how="left")

    # 3. 夜市營業時段（17–23）
    df["accident_datetime"] = pd.to_datetime(df["accident_datetime"], errors="coerce")
    df["hour"] = df["accident_datetime"].dt.hour
    df_night = df[(df["hour"] >= 17) & (df["hour"] <= 23)].copy()

    if df_night.empty:
        st.info(f"{selected_market} 在營業時段沒有事故資料。")
        return

    # 4. 天氣 / 光線 / 路面分類（向量化）
    df_night["is_rain"] = df_night["weather_condition"].astype(str).str.contains("雨", na=False).astype(int)
    df_night["is_dark"] = df_night["light_condition"].astype(str).str.contains("暗|夜|未開啟|無照明", na=False).astype(int)
    df_night["is_wet"] = df_night["road_surface_condition"].astype(str).str.contains("濕|積水", na=False).astype(int)

    # 5. 計算比例
    risk_df = pd.DataFrame({
        "risk": ["雨天", "光線不佳", "濕滑路面"],
        "value": [
            round(df_night["is_rain"].mean() * 100, 1),
            round(df_night["is_dark"].mean() * 100, 1),
            round(df_night["is_wet"].mean() * 100, 1),
        ]
    })

    # 6. 雷達圖
    fig = px.line_polar(
        risk_df,
        r="value",
        theta="risk",
        line_close=True,
        markers=True,
        range_r=[0, max(risk_df["value"]) * 1.2],
    )

    fig.update_traces(fill="toself", marker=dict(color="#FF6B6B", size=10))
    fig.update_layout(showlegend=False)

    st.plotly_chart(fig, use_container_width=True)
    st.markdown(""" *** """)
