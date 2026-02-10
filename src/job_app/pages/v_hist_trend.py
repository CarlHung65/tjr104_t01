import streamlit as st
import pandas as pd
import altair as alt
import sys
import os
import c_data_service as ds
import c_ui as ui

# 路徑設定
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import c_data_service as ds

st.set_page_config(layout="wide", page_title="歷年事故趨勢分析", page_icon="📈")

def main():
    df_market = ds.get_all_nightmarkets()
    ui.render_sidebar(df_market)
    st.title("📈 交通事故：歷年事故趨勢分析")
    st.markdown("透過下方篩選器，動態分析特定夜市在不同條件下的事故趨勢。")

    # 1. 取得資料庫清單
    df_market = ds.get_all_nightmarkets()
    
    # -------------------------------------------------------
    # 步驟 1 & 2: 區域與夜市篩選
    # -------------------------------------------------------
    with st.container(border=True):
        st.subheader("1️⃣ 鎖定目標")
        c1, c2 = st.columns(2)

        with c1:
            dist_opts = sorted(df_market['District'].dropna().unique())
            sel_dist = st.selectbox("選擇區域", dist_opts)

        with c2:
            filtered_market = df_market[df_market['District'] == sel_dist]
            market_opts = sorted(filtered_market['MarketName'].unique())
            sel_market_name = st.selectbox("選擇夜市", market_opts)
            
            # 取得座標
            target_market = filtered_market[filtered_market['MarketName'] == sel_market_name].iloc[0]

    # -------------------------------------------------------
    # 載入資料
    # -------------------------------------------------------
    # 使用 sample=False 抓全量資料
    df_raw, _, _, _ = ds.get_nearby_accidents(
        target_market['lat'], target_market['lon'], radius_km=0.5, sample=False)

    if df_raw.empty:
        st.warning("⚠️ 該夜市周邊無事故資料。")
        return

    # -------------------------------------------------------
    # 步驟 3 & 4: 資料維度篩選 (年份、天氣、肇因)
    # -------------------------------------------------------
    st.subheader("2️⃣ 條件篩選")
    
    with st.container(border=True):
        # 準備選單內容
        all_years = sorted(df_raw['Year'].unique())
        all_weather = sorted(df_raw['weather_condition'].dropna().unique())
        
        col_filter1, col_filter2 = st.columns(2)
        
        with col_filter1:
            sel_years = st.multiselect("📅 發生年份", all_years, default=all_years)
        
        with col_filter2:
            sel_weather = st.multiselect("☁️ 天氣狀況", all_weather, default=all_weather)

    # -------------------------------------------------------
    # 資料過濾邏輯
    # -------------------------------------------------------
    df_filtered = df_raw.copy()
    
    # 1. 年份篩選
    if sel_years:
        df_filtered = df_filtered[df_filtered['Year'].isin(sel_years)]
        
    # 2. 天氣篩選
    if sel_weather:
        df_filtered = df_filtered[df_filtered['weather_condition'].isin(sel_weather)]

    # 顯示目前資料量
    st.caption(f"📊 目前顯示筆數：{len(df_filtered)} 筆 (原始總數：{len(df_raw)} 筆)")
    st.markdown("---")

    # -------------------------------------------------------
    # 動態圖表呈現 (取代 PyGWalker)
    # -------------------------------------------------------
    
    if not df_filtered.empty:
        # [圖表1] 時間趨勢(折線圖)
        st.subheader("📈 事故發生趨勢 (月別)")
        
        # 整理數據：按年月統計
        trend_data = df_filtered.groupby('accident_datetime').size().reset_index(name='Count')
        # 為了畫圖好看，轉成年月格式
        trend_data['YearMonth'] = trend_data['accident_datetime'].dt.to_period('M').astype(str)
        trend_data = trend_data.groupby('YearMonth')['Count'].sum().reset_index()

        chart_trend = alt.Chart(trend_data).mark_line(point=True, color='#e74c3c').encode(
            x=alt.X('YearMonth:T', title='時間'),
            y=alt.Y('Count:Q', title='事故件數'),
            tooltip=['YearMonth', 'Count']).properties(height=300)
        
        st.altair_chart(chart_trend, use_container_width=True)

        c3, c4 = st.columns(2)

        # [圖表2] 前5大肇因(長條圖)
        with c3:
            st.subheader("🔥 主要肇因排行")
            if 'primary_cause' in df_filtered.columns:
                cause_data = df_filtered['primary_cause'].value_counts().head(5).reset_index()
                cause_data.columns = ['肇因', '件數']
                
                chart_bar = alt.Chart(cause_data).mark_bar(color='#f39c12').encode(
                    x=alt.X('件數:Q'),
                    y=alt.Y('肇因:N', sort='-x'), #按件數排序
                    tooltip=['肇因', '件數'])
                st.altair_chart(chart_bar, use_container_width=True)

        # [圖表3] 死傷嚴重程度(條形圖)
        with c4:
            st.subheader("🚑 死傷程度統計")
            severity_data = pd.DataFrame({
                '類別': ['受傷', '死亡'],
                '人數': [df_filtered['injury_count'].sum(), df_filtered['death_count'].sum()]})
            
            chart_sev = alt.Chart(severity_data).mark_bar().encode(
                x='人數:Q',
                y='類別:N',
                color=alt.Color('類別', scale=alt.Scale(domain=['死亡', '受傷'], range=['#000000', '#e67e22'])),
                tooltip=['類別', '人數'])
            st.altair_chart(chart_sev, use_container_width=True)

    else:
        st.info("👈 請調整篩選條件，目前無符合資料。")

if __name__ == "__main__":
    main()