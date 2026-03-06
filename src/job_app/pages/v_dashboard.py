import streamlit as st
from streamlit_folium import st_folium
import altair as alt
import sys
import os
import c_data_service as ds
import c_ui as ui
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) 
st.set_page_config(layout="wide", page_title="夜市區域事故分析", page_icon="📊")

def main():
    df_market = ds.get_all_nightmarkets()
    traffic_global = ds.get_taiwan_heatmap_data()
    st.session_state['show_accidents'] = True
    st.session_state['show_night_market'] = True
    is_overview, target_market, layers = ui.render_sidebar(df_market)

    # =========================================================
    # 頂部佈局：1/2/3 區塊並列
    # =========================================================
    st.markdown("""
        <div class="sticky-header">
            <h2 class="header-title" style="margin-bottom: 10px;">夜市區域事故分析</h2>
        </div>
    """, unsafe_allow_html=True)
    
    # 分配寬度比例：地點(24%) : 年份(24%) : KPI(52%)
    c_loc, c_year, c_kpi = st.columns([1.2, 1.2, 2.6], gap="large")

    # ---------------- 1️⃣ 第一區：選擇分析目標 ----------------
    with c_loc:
        st.markdown("📍 **1. 選擇目標**")
        
        # 設定預設值變數
        def_dist = "北部"
        def_city = "臺北市"
        def_market = "士林夜市"
        
        # 區域選單
        dist_opts = sorted(df_market['District'].dropna().unique())
        dist_idx = dist_opts.index(def_dist) if def_dist in dist_opts else 0
        sel_dist = st.selectbox("區域", dist_opts, index=dist_idx, key="d_dist", label_visibility="collapsed")
        
        # 縣市選單
        city_opts = sorted(df_market[df_market['District'] == sel_dist]['City'].dropna().unique())
        city_idx = city_opts.index(def_city) if (sel_dist == def_dist and def_city in city_opts) else 0
        sel_city = st.selectbox("縣市", city_opts, index=city_idx, key="d_city", label_visibility="collapsed")
        
        # 夜市選單
        m_opts = ["🔍 全台概覽 (預設)"] + sorted(df_market[df_market['City'] == sel_city]['MarketName'].dropna().unique())
        market_idx = m_opts.index(def_market) if (sel_city == def_city and def_market in m_opts) else 0
        sel_market = st.selectbox("夜市", m_opts, index=market_idx, key="d_market", label_visibility="collapsed")
        
    is_overview = (sel_market == "🔍 全台概覽 (預設)")
    target_market = None if is_overview else df_market[df_market['MarketName'] == sel_market].iloc[0]

    # --- 總覽模式 ---
    if is_overview:
        with c_year: st.info("👈 選擇夜市啟用年份篩選")
        with c_kpi: st.info("👈 選擇夜市後將顯示關鍵數據")
        st.markdown("---")
        m = ui.build_map(True, None, layers, None, 500, traffic_global, None, df_market)
        st_folium(m, height=700, width="stretch", returned_objects=[])
        return

    # --- 單一夜市模式 ---
    st.markdown("---")
    
    # 建立下方三欄式佈局 (地圖2 : 天氣1 : 肇因1)
    col_main, col_weather, col_cause = st.columns([2, 1, 1], gap="medium")
    
    # 在下方畫出滑桿取得數值
    with col_main:
        c_map_title, c_slider = st.columns([1, 1], vertical_alignment="bottom")
        with c_map_title:
            st.subheader(f"🗺️ {target_market['MarketName']} 事故熱點")
            radius_m = st.slider("📍 分析範圍 (m)", min_value=500, max_value=3000, step=500, value=1000)
            radius_km = radius_m / 1000.0

    # 載入數據
    with st.spinner(f"正在載入 {target_market['MarketName']} 周邊 {radius_m}m 事故資料..."):
        df_raw, _, _, yearly_stats_full = ds.get_nearby_accidents(
            target_market['lat'], target_market['lon'], radius_km=radius_km, sample=False
        )

    if df_raw.empty:
        st.warning("此區域暫無事故資料。")
        return

    # ---------------- 2️⃣ 第二區：分析年份 ----------------
    with c_year:
        st.markdown("📅 **2. 分析年份**")
        available_years = sorted(df_raw['Year'].unique(), reverse=True)
        default_years = available_years
        
        # 初始化「全選」的狀態
        if "d_chk_all" not in st.session_state:
            st.session_state["d_chk_all"] = True
            
        # 初始化各年份狀態
        for year in available_years:
            if f"d_chk_{year}" not in st.session_state:
                st.session_state[f"d_chk_{year}"] = (year in default_years)

        def toggle_all():
            is_checked = st.session_state.get("d_chk_all", False)
            for y in available_years: st.session_state[f"d_chk_{y}"] = is_checked

        st.checkbox("全選", key="d_chk_all", on_change=toggle_all)
        
        y_cols = st.columns(2)
        selected_years = []
        for i, year in enumerate(available_years):
            with y_cols[i % 2]:
                if st.checkbox(str(year), key=f"d_chk_{year}"):
                    selected_years.append(year)

    if not selected_years:
        with c_kpi: st.error("⚠️ 請至少選擇一個年份。")
        return
    
    df_filtered = df_raw[df_raw['Year'].isin(selected_years)]

    # ---------------- 3️⃣ 第三區：關鍵指標 (KPI) ----------------
    with c_kpi:
        st.markdown("📊 **3. 關鍵指標**")
        stats_new = {
            "total": len(df_filtered),
            "dead": int(df_filtered['death_count'].sum()),
            "hurt": int(df_filtered['injury_count'].sum())
        }
        weather_grp = df_filtered.groupby('weather_condition').agg(件數=('accident_datetime', 'count')).reset_index()
        weather_grp.columns = ['天氣', '件數']
        rain_count = weather_grp[weather_grp['天氣'].astype(str).str.contains('雨')]['件數'].sum() if not weather_grp.empty else 0
        rain_ratio = (rain_count / stats_new['total']) * 100 if stats_new['total'] > 0 else 0

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("📌 事故數", f"{stats_new['total']} 件")
        k2.metric("💀 死亡人數", f"{stats_new['dead']} 人")
        k3.metric("🚑 受傷人數", f"{stats_new['hurt']} 人")
        k4.metric("🌧️ 雨天比例", f"{rain_ratio:.1f}%")

    
    st.markdown("---")

    # =========================================================
    # 三欄式：地圖(2)|天氣(1)|肇因/時段(1)
    # =========================================================
    
    col_main, col_weather, col_cause = st.columns([2, 1, 1], gap="medium")
    
    # --- 左欄：地圖 ---
    with col_main:
        # 1. 確保死亡事故優先保留，不隨機抽樣
        if len(df_filtered) > 1000:
            df_for_map = df_filtered.sort_values(by=['death_count', 'injury_count'], ascending=False).head(1000)
        else:
            df_for_map = df_filtered

        # 2. 根據滑桿半徑動態計算縮放級別
        if radius_m <= 500:
            d_zoom = 16
        elif radius_m <= 1000:
            d_zoom = 15
        elif radius_m <= 2000:
            d_zoom = 14
        elif radius_m <= 3000:
            d_zoom = 13
        else:
            d_zoom = 12

        # 3. 將 d_zoom 傳入地圖，並將 use_container_width 修正為最新語法 width="stretch"
        m = ui.build_map(False, target_market, layers, d_zoom, radius_m, None, df_for_map, df_market)
        st_folium(m, height=500, width="stretch", returned_objects=[])

    # --- 中欄：天候風險 ---
    with col_weather:
        st.subheader("☂️ 天候風險")
        if not weather_grp.empty:
            # 圓餅圖 + 標籤
            base_pie = alt.Chart(weather_grp).encode(theta=alt.Theta("件數", stack=True))
            pie = base_pie.mark_arc(innerRadius=40).encode(
                color=alt.Color("天氣", scale=alt.Scale(scheme='tableau10')),
                tooltip=['天氣', '件數'])
            
            pie_text = base_pie.mark_text(radius=80).encode(
                text="件數", order=alt.Order("天氣"), color=alt.value("black"))
            
            st.altair_chart((pie + pie_text).properties(height=220), use_container_width=True)
            
            # 堆疊長條圖 + 標籤
            st.subheader("☠️ 死傷程度") 
            weather_sev = df_filtered.groupby('weather_condition').agg(
                死亡=('death_count', 'sum'), 受傷=('injury_count', 'sum')
            ).reset_index().rename(columns={'weather_condition': '天氣'})
            df_melt = weather_sev.melt(id_vars=['天氣'], value_vars=['死亡', '受傷'], var_name='類別', value_name='人數')
            df_melt = df_melt[df_melt['人數'] > 0]
            
            base_bar = alt.Chart(df_melt).encode(
                x=alt.X('天氣:N', sort='-x', title=None),
                y=alt.Y('人數:Q'),
                color=alt.Color('類別:N', scale=alt.Scale(range=["#3157BE", "#FF1616"])),)
            bar = base_bar.mark_bar()
            text = base_bar.mark_text(dy=-10, color='black').encode(text='人數:Q')
            st.altair_chart((bar + text).properties(height=200), use_container_width=True)
        else:
            st.info("無數據")

    # --- 右欄：肇因與時段 ---
    with col_cause:
        st.subheader("🔍 肇因分析")
        
        # 肇因圖
        if 'primary_cause' in df_filtered.columns:
            df_cause = df_filtered['primary_cause'].value_counts().head(5).reset_index() # 取前5大
            df_cause.columns = ['肇因', '件數']
            
            base_c = alt.Chart(df_cause).encode(
                x=alt.X('件數:Q'),
                y=alt.Y('肇因:N', sort='-x', axis=alt.Axis(labels=True, title=None)), # 肇因名稱顯示
                tooltip=['肇因', '件數'])
            
            bar_c = base_c.mark_bar().encode(color=alt.Color('件數:Q', scale=alt.Scale(scheme='reds'), legend=None))
            text_c = base_c.mark_text(align='left', dx=2).encode(text='件數:Q')
            st.altair_chart((bar_c + text_c).properties(height=250), use_container_width=True)

        st.subheader("🌙 24H 熱力")
        if 'Hour' in df_filtered.columns:
            df_hour = df_filtered.groupby('Hour').size().reset_index(name='件數')
            chart_hour = alt.Chart(df_hour).mark_area(
                color='lightblue', line={'color':'darkblue'}).encode(
                x=alt.X('Hour:O', title='hr'),
                y=alt.Y('件數:Q', title=None),
                tooltip=['Hour', '件數']).properties(height=180)
            st.altair_chart(chart_hour, use_container_width=True)

    # with st.expander("📄 查看原始歷年統計表"):
    #     st.dataframe(yearly_stats_full, use_container_width=True)

if __name__ == "__main__":
    main()