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
    st.session_state['show_accidents'] = True
    st.session_state['show_night_market'] = True
    is_overview, target_market, layers = ui.render_sidebar(df_market)

    # --- 總覽模式 ---
    if is_overview:
        st.title("📊 夜市區域事故分析")
        st.info("👈 請從左側選單選擇一個 **夜市**，即可啟用年份篩選與詳細分析。")
        traffic_global = ds.get_taiwan_heatmap_data()
        m = ui.build_map(True, None, layers, None, traffic_global, None, df_market)
        st_folium(m, height=700, use_container_width=True, returned_objects=[])
        return

    # --- 單一夜市模式 ---
    # 標頭
    st.markdown(f"""
        {target_market['City']} {target_market['District']}
        <h1 class="header-title">🚘 {target_market['MarketName']}：夜市區域事故分析</h1>
        </div>
        範圍：500m
        </div>
    """, unsafe_allow_html=True)

    # 載入數據
    with st.spinner(f"正在載入 {target_market['MarketName']} 完整事故資料..."):
        df_raw, _, _, yearly_stats_full = ds.get_nearby_accidents(
            target_market['lat'], target_market['lon'], radius_km=0.5, sample=False)

    if df_raw.empty:
        st.warning("此區域暫無事故資料。")
        return

    # 年份篩選
    available_years = sorted(df_raw['Year'].unique(), reverse=True)
    with st.container():
        c_filter, c_pad = st.columns([3, 1])
        with c_filter:
            selected_years = st.multiselect(
                "📅 請選擇分析年份 (可多選，預設全選):",
                options=available_years,
                default=available_years,
                placeholder="請選擇年份...")

    if not selected_years:
        st.error("⚠️ 請至少選擇一個年份。")
        return

    df_filtered = df_raw[df_raw['Year'].isin(selected_years)]

    # 計算數據
    stats_new = {
        "total": len(df_filtered),
        "dead": int(df_filtered['death_count'].sum()),
        "hurt": int(df_filtered['injury_count'].sum())}
    
    weather_grp = df_filtered.groupby('weather_condition').agg(
        件數=('accident_datetime', 'count'),
        死亡=('death_count', 'sum'),
        受傷=('injury_count', 'sum')
    ).reset_index().rename(columns={'weather_condition': '天氣'})

    # 5️⃣ KPI 卡片
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("📊 選定範圍事故數", f"{stats_new['total']} 件")
    k2.metric("💀 死亡人數", f"{stats_new['dead']} 人")
    k3.metric("🚑 受傷人數", f"{stats_new['hurt']} 人")
    
    rain_count = weather_grp[weather_grp['天氣'].astype(str).str.contains('雨')]['件數'].sum() if not weather_grp.empty else 0
    rain_ratio = (rain_count / stats_new['total']) * 100 if stats_new['total'] > 0 else 0
    k4.metric("🌧️ 雨天事故率", f"{rain_ratio:.1f}%")
    
    st.markdown("---")

    # =========================================================
    # 三欄式：地圖(2)|天氣(1)|肇因/時段(1)
    # =========================================================
    
    col_main, col_weather, col_cause = st.columns([2, 1, 1], gap="medium")
    
    # --- 左欄：地圖 ---
    with col_main:
        st.subheader("🗺️ 事故熱點地圖")
        m = ui.build_map(False, target_market, layers, None, None, df_filtered, df_market)
        # 加上 returned_objects=[] 防止重整
        st_folium(m, height=500, use_container_width=True, returned_objects=[])

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
            st.markdown("##### ☠️ 死傷程度")
            df_melt = weather_grp.melt(id_vars=['天氣'], value_vars=['死亡', '受傷'], var_name='類別', value_name='人數')
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

        st.markdown("##### 🌙 24H 熱力")
        if 'Hour' in df_filtered.columns:
            df_hour = df_filtered.groupby('Hour').size().reset_index(name='件數')
            chart_hour = alt.Chart(df_hour).mark_area(
                color='lightblue', line={'color':'darkblue'}).encode(
                x=alt.X('Hour:O', title='hr'),
                y=alt.Y('件數:Q', title=None),
                tooltip=['Hour', '件數']).properties(height=180)
            st.altair_chart(chart_hour, use_container_width=True)

    with st.expander("📄 查看原始歷年統計表"):
        st.dataframe(yearly_stats_full, use_container_width=True)

if __name__ == "__main__":
    main()