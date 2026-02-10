import streamlit as st
from streamlit_folium import st_folium
import c_data_service as ds
import c_ui as ui

# 1. 頁面設定
st.set_page_config(
    layout="wide", 
    page_title="台灣夜市風險地圖", 
    initial_sidebar_state="expanded")

def main():
    # 取得全台資料 (熱力圖數據 & 夜市清單)
    traffic_global = ds.get_taiwan_heatmap_data()
    df_market = ds.get_all_nightmarkets()

    # 呼叫側邊欄 (雖然是首頁，但保留導航功能)
    is_overview, target_market, layers = ui.render_sidebar(df_market)

    # 版面配置 (左:文字介紹 / 右:全台地圖)
    col_text, col_map = st.columns([1, 2], gap="large")

    # --- 左欄：文字介紹 ---
    with col_text:
        st.title(f"台灣夜市與交通事故風險地圖")
        st.write("---")
        st.markdown("### 🎯 專案動機")
        st.markdown("""
        #### 🚦 為什麼我們關注夜市安全？
        外媒直指臺灣街頭是 **「行人地獄」**，交通事故死亡人數驚人。城市規劃偏車不偏人、駕駛陋習成常態，讓過馬路像賭命！
        
        <div style="background-color: #f0f2f6; padding: 15px; border-radius: 5px; color: #222; font-weight: 500; margin-top: 10px; border-left: 4px solid #ff4b4b;">
        我們用數據工程打造互動地圖：解析肇因比例、即時告警、用路人評論，甚至串接天氣/人流/節慶，揭露雨夜塞車、寒流車速亂飆的隱藏殺機。
        </div>
        """, unsafe_allow_html=True)

        st.write("---")
        st.markdown("""
        ### 🌐 功能指引
        * **🏠 首頁 (目前頁面)**: 觀看全台交通事故熱區分佈。
        * **📊 夜市區域事故分析**: 進入單一夜市，查看詳細風險分析 (肇因、時段、趨勢)。
        * **📈 歷年趨勢分析**: 比較不同年份的數據變化。     
        """)
        st.write("---")
        st.markdown("### 📊 資料來源")
        st.info("""
        * **交通事故資料**: 政府資料開放平台（交通部公開資料，2021–2024）
        * **氣象即時資料**: 中央氣象署 CWA OpenData API
        * **氣象歷史資料**: 中央氣象署 CODiS（氣候觀測資料查詢系統）
        * **夜市相關資料**: Google Maps Platform API（Places API)
        * **地圖底圖**: OpenStreetMap (OSM)
        """)
        
    # --- 右欄：全台地圖 ---
    with col_map:
        # 在右欄上方建立兩欄，左邊放標題，右邊放提示
        c_head, c_hint = st.columns([1.8, 1.8])
        
        with c_head:
            st.subheader("🇹🇼 全台即時概覽")
            
        with c_hint:
            # 這是您想要移過來的提示，放在右上方比較顯眼
            st.info("提示：請點擊左側「導航選單」來進行詳細分析。", icon="💡")

        st.write("""
           此處顯示全台目前的 **交通熱區 (紅/黃色區塊)** 與 **夜市分佈 (紫色點)**""")
        
        # 首頁強制設定：只顯示「熱力圖」和「夜市」，隱藏其他細節
        home_layers = {
            'traffic_heat': True,
            'night_market': True,
            'weather': False,
            'stations': False,
            'accidents': False }

        # 建立地圖物件 (is_overview=True)
        m = ui.build_map(
            is_overview = True,
            target_market = None,
            layers = home_layers,
            weather_data = None,
            traffic_global = traffic_global,
            df_local = None,
            df_market = df_market)

        # 顯示地圖
        st_folium(m, height=800, use_container_width=True, returned_objects=[])

if __name__ == "__main__":
    main()