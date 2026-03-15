import streamlit as st
import sys
import os
import pandas as pd
import altair as alt
import core.c_data_service as ds
import core.c_ui as ui
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


st.set_page_config(layout="wide", page_title="交通政策影響分析", page_icon="⚖️")

def main():
    df_market = ds.get_all_nightmarkets()
    ui.render_sidebar(df_market)
    st.title("⚖️ 禮讓行人政策成效分析")
    with st.expander("📝 專案願景：從小民到政策，為何我們必須改變？", expanded=True):
        st.markdown("""
        #### 👴 高齡危機來襲
        臺灣人口加速老化，高齡駕駛與行人激增，行動遲緩、判斷力衰退，上路風險飆升——**你我終將老去，豈能坐視不管？**
        
        #### 🛡️ 從數據驅動改變
        我們希望從夜市周邊逆轉惡名，提升在地安全、化解遊客疑慮，進而影響政策。
        透過數據驗證政策成效，讓**人人參與**，期許臺灣交通不再是「地獄」，而是驕傲的「好行」！
        """)
    st.markdown("針對 **2023 年 6 月 30 日** 起實施的《道路交通管理處罰條例》修正案，整合全台區域分佈與個別夜市趨勢。")
    st.markdown("---")


    # =========================================================
    # 1. 綜合圖表：全台月別趨勢 (堆疊長條 + 總量折線)
    # =========================================================
    st.subheader("1️⃣ 全台行人事故：區域分佈與總體趨勢")
    
    # 取得「月別 + 區域」的詳細資料
    df_region_month = ds.get_pedestrian_stats_by_region_monthly()
    
    if not df_region_month.empty:
        # [優化 1] 資料前處理：將 YearMonth 字串轉為 datetime 物件
        # 這樣 Altair 就能自動優化 X 軸的標籤顯示，不會擠在一起
        df_region_month['Date'] = pd.to_datetime(df_region_month['YearMonth'])
        
        # 計算全台每月的「總計」
        df_total = df_region_month.groupby('Date')['Count'].sum().reset_index()
        
        # [優化 2] 計算移動平均 (3個月)，讓趨勢線更平滑，不被單月雜訊干擾
        df_total['RollingMean'] = df_total['Count'].rolling(window=3, min_periods=1).mean()
        
        policy_date = pd.to_datetime('2023-06-01')
        
        # [圖層1] 堆疊面積圖 (Area Chart) - 取代長條圖
        # opacity=0.7 讓顏色通透一點，不會太搶眼
        area = alt.Chart(df_region_month).mark_area(opacity=0.7).encode(
            x=alt.X('Date:T', title='年份', axis=alt.Axis(format='%Y-%m')), # T 代表時間格式
            y=alt.Y('Count:Q', title='事故件數', stack=True),
            color=alt.Color('Region:N', title='區域', scale=alt.Scale(scheme='category10')),
            tooltip=[
                alt.Tooltip('YearMonth', title='時間'),
                alt.Tooltip('Region', title='區域'),
                alt.Tooltip('Count', title='件數')
            ]
        )
        
        # [圖層2] 趨勢折線圖 (顯示移動平均)
        # 改用深紅色顯示趨勢，並加粗線條
        line = alt.Chart(df_total).mark_line(color='#c0392b', strokeWidth=3).encode(
            x='Date:T',
            y=alt.Y('RollingMean:Q'),
            tooltip=[
                alt.Tooltip('Date', title='時間', format='%Y-%m'),
                alt.Tooltip('RollingMean', title='3個月平均', format='.1f')
            ]
        )
        
        # [圖層3] 政策垂直線
        rule = alt.Chart(pd.DataFrame({'Date': [policy_date]})).mark_rule(
            color='black', strokeDash=[5, 5], strokeWidth=2
        ).encode(x='Date:T')
        
        # [圖層4] 文字標籤
        text = alt.Chart(pd.DataFrame({
            'Date': [policy_date], 
            'Label': [' 政策實施'], 
            'Y': [df_total['Count'].max()] # 文字顯示在圖表上方
        })).mark_text(
            align='left', dx=5, color='black', fontSize=14, fontWeight='bold'
        ).encode(
            x='Date:T', 
            y='Y:Q', 
            text='Label'
        )

        # 組合所有圖層
        combined_chart = (area + line + rule + text).resolve_scale(y='shared').properties(height=450)
        
        st.altair_chart(combined_chart, use_container_width=True)
        
        # 簡單數據卡片 (維持不變)
        avg_before = df_total[df_total['Date'] < policy_date]['Count'].mean()
        avg_after = df_total[df_total['Date'] >= policy_date]['Count'].mean()
        
        c1, c2 = st.columns(2)
        c1.metric("政策前全台平均 (月)", f"{avg_before:.0f} 件")
        c2.metric("政策後全台平均 (月)", f"{avg_after:.0f} 件", delta=f"{avg_after - avg_before:.0f}")

    st.markdown("---")

    # =========================================================
    # 2. 下半部：特定夜市比較 (分區勾選)
    # =========================================================
    st.subheader("2️⃣ 特定夜市比較分析 (分區篩選)")
    st.caption("請從下方各區域勾選夜市，進行多點趨勢比較。")

    # 準備資料：將夜市按區分類
    regions = ['北部', '中部', '南部', '東部'] 
    
    selected_markets_all = []
    
    # 建立 4 個欄位來放選單
    cols = st.columns(4)
    
    for idx, region in enumerate(regions):
        with cols[idx]:
            st.markdown(f"**{region}**")
            # 篩選該區夜市
            if region == '東部':
                # 簡單邏輯：非北中南就算東部 (依照資料庫欄位內容調整)
                m_list = df_market[~df_market['District'].isin(['北部', '中部', '南部'])]['MarketName'].dropna().unique()
                if len(m_list) == 0: # 如果資料庫有寫 '東部'
                     m_list = df_market[df_market['District'] == '東部']['MarketName'].dropna().unique()
            else:
                m_list = df_market[df_market['District'] == region]['MarketName'].dropna().unique()
            
            # 排序
            m_list = sorted(m_list)
            
            # 預設勾選 (範例)
            defaults = []
            if region == '北部' and '士林夜市' in m_list: defaults = ['士林夜市']
            if region == '中部' and '逢甲夜市' in m_list: defaults = ['逢甲夜市']
            if region == '南部' and '羅東觀光夜市' in m_list: defaults = [] 
            
            # 建立多選單
            selections = st.multiselect(f"選擇{region}夜市", m_list, default=defaults, key=f"sel_{region}")
            selected_markets_all.extend(selections)

    # 繪圖邏輯
    if selected_markets_all:
        st.markdown("##### 📊 選定夜市趨勢比較圖")
        comp_data = []
        
        # 顯示進度條，因為選多個可能會慢
        my_bar = st.progress(0, text="查詢數據中...")
        total_sel = len(selected_markets_all)
        
        for i, m_name in enumerate(selected_markets_all):
            # 查詢數據
            row = df_market[df_market['MarketName'] == m_name].iloc[0]
            df_temp = ds.get_pedestrian_trend(row['lat'], row['lon'])
            
            if not df_temp.empty:
                df_temp['Market'] = m_name
                comp_data.append(df_temp)
            
            my_bar.progress((i + 1) / total_sel)
            
        my_bar.empty() # 清除進度條
        
        if comp_data:
            df_comp = pd.concat(comp_data)
            policy_date = '2023-06'
            
            # 繪製多線圖
            chart_bottom = alt.Chart(df_comp).mark_line(point=True).encode(
                x=alt.X('YearMonth:O', title='年月'),
                y=alt.Y('Count:Q', title='事故件數'),
                color=alt.Color('Market:N', title='夜市名稱'),
                tooltip=['YearMonth', 'Market', 'Count']
            ).properties(height=400)
            
            # 加上政策線
            rule_bottom = alt.Chart(pd.DataFrame({'Date': [policy_date]})).mark_rule(color='red', strokeDash=[5, 5]).encode(x='Date:O')
            
            st.altair_chart(chart_bottom + rule_bottom, use_container_width=True)
        else:
            st.warning("所選夜市周邊暫無足夠的行人事故數據。")
    else:
        st.info("👈 請上方勾選至少一個夜市以開始比較。")

if __name__ == "__main__":
    main()