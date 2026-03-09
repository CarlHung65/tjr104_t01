import folium
import streamlit as st
import pandas as pd
import numpy as np
from folium.plugins import MarkerCluster
from folium.plugins import HeatMap

@st.cache_data
def build_map(
    map_type: str,
    nm_df,
    center_lat: float,
    center_lon: float,
    plot_df=None,
    extra=None
):

    # -----------------------------------------------------
    # ⭐ 建立地圖
    # -----------------------------------------------------
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=16,
        tiles="OpenStreetMap"
    )

    # -----------------------------------------------------
    # ⭐ 夜市框線（如果有提供）
    # -----------------------------------------------------
    if extra and "bounds" in extra:
        sw_lat, sw_lon, ne_lat, ne_lon = extra["bounds"]
        folium.Rectangle(
            bounds=[[sw_lat, sw_lon], [ne_lat, ne_lon]],
            color="#FF8C42",
            weight=2,
            fill=True,
            fill_opacity=0.15
        ).add_to(m)

    # -----------------------------------------------------
    # ⭐ 夜市圓形 + 標籤（所有地圖都會用）
    # -----------------------------------------------------
    if map_type != "pdi_heat":
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

    # -----------------------------------------------------
    # ⭐ Act 1
    # -----------------------------------------------------

    # -----------------------------------------------------
    # 1. 白天 / 夜間 熱力圖（圓形 + 標籤 + 事故點）
    # -----------------------------------------------------
    if map_type in ("day_heat", "night_heat"):

        layer = folium.FeatureGroup(name=map_type)

        # 熱力圖
        HeatMap(
            plot_df[["latitude", "longitude", "heat_weight"]].values.tolist(),
            radius=18,
            blur=15,
            min_opacity=0.3
        ).add_to(layer)

        layer.add_to(m)

        # 事故點
        for _, row in plot_df.iterrows():

            if row["death_count"] > 0:
                color = "#FF1744"
            elif row["injury_count"] > 0:
                color = "#0033cc"
            else:
                color = "#90CAF9"

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
        return m
            
    # -----------------------------------------------------
    # 2. PDI 熱力圖（依事故 PDI 權重）
    # -----------------------------------------------------
    elif map_type == "pdi_heat":

        pdi_points = extra["pdi_points"]   # [[lat, lon, weight], ...]
        bounds = extra["bounds"]           # (sw_lat, sw_lon, ne_lat, ne_lon)

        # 熱力圖
        HeatMap(
            pdi_points,
            radius=18,
            max_zoom=15,
            blur=20,
            max_val=max([p[2] for p in pdi_points]) if pdi_points else 1
        ).add_to(m)

        # 夜市框線
        sw_lat, sw_lon, ne_lat, ne_lon = bounds
        folium.Rectangle(
            bounds=[[sw_lat, sw_lon], [ne_lat, ne_lon]],
            color="#FF8C42",
            weight=2,
            fill=True,
            fill_opacity=0.15
        ).add_to(m)
        return m 


def build_map_cluster(selected_market, date_filtered_df, nm_df, cluster_mode):
    # 夜市中心
    nm = nm_df[nm_df["nightmarket_name"] == selected_market].iloc[0]
    center_lat, center_lon = nm["nightmarket_latitude"], nm["nightmarket_longitude"]

    # -----------------------------------------------------
    # 1. 處理事故資料（向量化）
    # -----------------------------------------------------
    df = date_filtered_df.copy()
    df["accident_datetime"] = pd.to_datetime(df["accident_datetime"], errors="coerce")
    df["hour"] = df["accident_datetime"].dt.hour
    df["is_night"] = df["hour"].apply(lambda h: h < 6 or h >= 18)

    # -----------------------------------------------------
    # 2. 建立地圖
    # -----------------------------------------------------
    m = folium.Map(location=[center_lat, center_lon], zoom_start=17, tiles="OpenStreetMap")

    # 夜市圈
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

    # -----------------------------------------------------
    # 3. 建立群聚（只跑一次迴圈）
    # -----------------------------------------------------
    day_cluster = MarkerCluster()
    night_cluster = MarkerCluster()

    for _, row in df.iterrows():
        tooltip_text = f"""
        <b>事故時間：</b>{row['accident_datetime']}<br>
        <b>死亡：</b>{row['death_count']}<br>
        <b>受傷：</b>{row['injury_count']}<br>
        """

        marker = folium.Marker(
            location=[row["latitude"], row["longitude"]],
            tooltip=tooltip_text,
            icon=folium.Icon(
                color="blue" if row["is_night"] else "orange",
                icon="info-sign"
            )
        )

        if row["is_night"]:
            marker.add_to(night_cluster)
        else:
            marker.add_to(day_cluster)

    # -----------------------------------------------------
    # 4. 顯示使用者選擇的群聚
    # -----------------------------------------------------
    if cluster_mode == "☀️ 白天事故群聚":
        day_cluster.add_to(m)
    else:
        night_cluster.add_to(m)

    return m

    # -----------------------------------------------------
    #  事故相關 — 全台事故密度地圖 (Density Map) 
    # -----------------------------------------------------
    
def build_taiwan_density_map(df):

   # 台灣中心點
    taiwan_center = [23.7, 120.97]

    m = folium.Map(
        location=taiwan_center,
        zoom_start=7.4,
        tiles="OpenStreetMap"
    )

    # ⭐ 效能提升：向量化權重
    death_mask = df["death_count"] > 0
    injury_mask = df["injury_count"] > 0

    df = df.copy()
    df["heat_weight"] = np.where(death_mask, 5,
                            np.where(injury_mask, 2, 1))

    heat_data = df[["latitude", "longitude", "heat_weight"]].values.tolist()

    HeatMap(
        heat_data,
        radius=18,
        blur=15,
        min_opacity=0.3
    ).add_to(m)

    return m
