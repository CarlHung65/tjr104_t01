import streamlit as st
import folium
from folium.plugins import HeatMap, MarkerCluster
import pandas as pd

# ==========================================
# 側邊欄
# ==========================================
def set_layers_state(is_active):
    keys = ['show_traffic_heat', 'show_night_market', 'show_weather', 'show_accidents']
    for k in keys:
        st.session_state[k] = is_active

def render_sidebar(df_market):
    """
    側邊欄: 區域(北中南) -> 縣市 -> 夜市 的篩選邏輯 (預設為「士林夜市」)
    """
    st.sidebar.markdown("### 導航選單")
    st.sidebar.page_link("r_app.py", label="首頁", icon="🏠")
    st.sidebar.page_link("pages/v_dashboard.py", label="夜市區域事故分析", icon="📊")
    st.sidebar.page_link("pages/v_hist_trend.py", label="歷年事故趨勢分析", icon="📈")
    st.sidebar.page_link("pages/v_policy_impact.py", label="交通政策影響分析", icon="⚖️")
    st.sidebar.markdown("---")
    st.sidebar.header("🔍 篩選導航")

    # 初始化圖層狀態
    st.session_state.setdefault("show_traffic_heat", False)
    st.session_state.setdefault("show_night_market", True) 
    st.session_state.setdefault("show_weather", False)
    st.session_state.setdefault("show_accidents", True) 
    # 初始化導航預設值
    if 'nav_district' not in st.session_state: st.session_state['nav_district'] = "北部"
    if 'nav_city' not in st.session_state: st.session_state['nav_city'] = "臺北市"
    if 'nav_market' not in st.session_state: st.session_state['nav_market'] = "士林夜市"

    # [區域選單]
    dist_opts = sorted(df_market['District'].unique()) if not df_market.empty else []
    dist_opts = [x for x in dist_opts if x.lower() not in ['nan', 'none', '']]
    
    if st.session_state['nav_district'] not in dist_opts and dist_opts:
        st.session_state['nav_district'] = dist_opts[0]

    def update_dist():
        st.session_state['nav_district'] = st.session_state['w_dist']
        filtered = df_market[df_market['District'] == st.session_state['w_dist']]
        if not filtered.empty:
            valid_cities = sorted(filtered['City'].unique())
            if valid_cities:
                st.session_state['nav_city'] = valid_cities[0]

    sel_dist = st.sidebar.selectbox(
        "1️⃣ 區域", 
        dist_opts, 
        index=dist_opts.index(st.session_state['nav_district']) if st.session_state['nav_district'] in dist_opts else 0,
        key='w_dist',
        on_change=update_dist)

    # [縣市選單]
    df_city_filtered = df_market[df_market['District'] == sel_dist]
    city_opts = sorted(df_city_filtered['City'].unique()) if not df_city_filtered.empty else []
    
    if st.session_state['nav_city'] not in city_opts and city_opts:
        st.session_state['nav_city'] = city_opts[0]

    def update_city():
        st.session_state['nav_city'] = st.session_state['w_city']

    sel_city = st.sidebar.selectbox(
        "2️⃣ 縣市", 
        city_opts, 
        index=city_opts.index(st.session_state['nav_city']) if st.session_state['nav_city'] in city_opts else 0,
        key='w_city',
        on_change=update_city)

    # [夜市選單]
    df_m = df_city_filtered[df_city_filtered['City'] == sel_city]
    m_opts = sorted(df_m['MarketName'].unique())
    
    if st.session_state['nav_market'] not in m_opts and m_opts:
        st.session_state['nav_market'] = m_opts[0]
    
    def update_market():
        st.session_state['nav_market'] = st.session_state['w_market']

    sel_market = st.sidebar.selectbox(
        "3️⃣ 夜市", 
        m_opts, 
        index=m_opts.index(st.session_state['nav_market']), 
        key='w_market', 
        on_change=update_market)

    # --- 圖層控制 ---
    st.sidebar.markdown("---")
    st.sidebar.subheader("🗺️ 圖層控制")
    
    c1, c2 = st.sidebar.columns(2)
    c1.button("✅ 全選", on_click=set_layers_state, args=(True,), use_container_width=True)
    c2.button("⬜ 取消", on_click=set_layers_state, args=(False,), use_container_width=True)

    layers = {
        "traffic_heat": st.sidebar.checkbox("🔥 全台車禍熱區", key='show_traffic_heat'),
        "night_market": st.sidebar.checkbox("🏠 夜市位置", key='show_night_market'),
        "weather": st.sidebar.checkbox("🌧️ 降雨熱力", key='show_weather'),
        "accidents": st.sidebar.checkbox("🔵 周邊事故詳情", key='show_accidents')}
    
    is_overview = (sel_market == "🔍 全台概覽")
    target_market = None
    if not is_overview and not df_m.empty:
        target = df_m[df_m['MarketName'] == sel_market]
        if not target.empty: target_market = target.iloc[0]
            
    return is_overview, target_market, layers

# ==========================================
# 地圖
# ==========================================
def build_map(is_overview, target_market, layers, weather_data, traffic_global, df_local, df_market):
    if is_overview: 
        loc, zoom = [23.7, 120.95], 8
    elif target_market is not None: 
        loc, zoom = [target_market['lat'], target_market['lon']], 16
    else: 
        loc, zoom = [25.03, 121.56], 12

    m = folium.Map(location=loc, zoom_start=zoom, tiles="CartoDB positron", prefer_canvas=True)

    if layers.get('traffic_heat') and traffic_global:
        HeatMap(traffic_global, radius=15, blur=12, min_opacity=0.3).add_to(m)

    if layers.get('night_market'):
        fg_m = folium.FeatureGroup(name="夜市")
        if target_market is not None:
            folium.Marker([target_market['lat'], target_market['lon']], icon=folium.Icon(color='purple', icon='star', prefix='fa'), tooltip=target_market['MarketName']).add_to(fg_m)
            folium.Circle([target_market['lat'], target_market['lon']], radius=500, color='orange', fill=True, fill_opacity=0.1).add_to(fg_m)
        else:
            for _, r in df_market.iterrows():
                folium.CircleMarker([r['lat'], r['lon']], radius=3, color='purple', tooltip=r['MarketName']).add_to(fg_m)
        fg_m.add_to(m)

    if not is_overview and layers.get('accidents') and df_local is not None and not df_local.empty:
        df_death = df_local[df_local['death_count'] > 0]
        df_other = df_local[df_local['death_count'] == 0]

        # 將一般事故放入專屬圖層
        fg_other = folium.FeatureGroup(name="一般事故")
        if len(df_other) > 800:
            heat_data = [[r.latitude, r.longitude] for r in df_other.itertuples()]
            folium.plugins.HeatMap(heat_data, radius=12, blur=15, min_opacity=0.3).add_to(fg_other)
        else:
            cluster_other = MarkerCluster(maxClusterRadius=30, disableClusteringAtZoom=16).add_to(fg_other)
            for r in df_other.itertuples():
                i_count = getattr(r, 'injury_count', 0)
                color = 'blue' if i_count > 0 else 'black'
                cause = getattr(r, 'primary_cause', '未知')
                
                dt = getattr(r, 'accident_datetime', None)
                dt_str = dt.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(dt) else '未知時間'
                
                popup_text = f"一般事故<br>{dt_str}<br>{cause}<br>傷:{i_count}"
                folium.CircleMarker(
                    [r.latitude, r.longitude], 
                    radius=5, color=color, fill=True, fill_opacity=0.7, 
                    popup=folium.Popup(popup_text, max_width=200)
                ).add_to(cluster_other)
        fg_other.add_to(m)

        # 將死亡事故放入另一個專屬圖層
        if not df_death.empty:
            fg_death = folium.FeatureGroup(name="死亡事故", show=True)
            for r in df_death.itertuples():
                d_count = getattr(r, 'death_count', 0)
                i_count = getattr(r, 'injury_count', 0)
                
                dt = getattr(r, 'accident_datetime', None)
                dt_str = dt.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(dt) else '未知時間'
                cause = getattr(r, 'primary_cause', '未知')
                
                popup_text = f"🚨 死亡事故<br>{dt_str}<br>{cause}<br>死:{d_count} 傷:{i_count}"
                
                # 使用 HTML DivIcon 取代 CircleMarker，強制提升 Z-index 到最頂層
                icon_html = '<div style="background-color: #ff0000; width: 16px; height: 16px; border-radius: 50%; border: 2px solid white; box-shadow: 0 0 6px rgba(0,0,0,0.8);"></div>'
                
                folium.Marker(
                    [r.latitude, r.longitude], 
                    icon=folium.DivIcon(html=icon_html, icon_anchor=(8, 8)),
                    popup=folium.Popup(popup_text, max_width=200),
                    z_index_offset=1000 # 強制永遠顯示在其他點位之上
                ).add_to(fg_death)
                
            fg_death.add_to(m)
            
        # 加入圖層控制面板 (地圖右上角)
        folium.LayerControl(collapsed=False).add_to(m)

    return m # 將畫好的地圖交還給主程式
