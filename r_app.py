import streamlit as st
# 🌟 保留原註解：因為目前不顯示地圖，先將 st_folium 註解掉以節省資源
# from streamlit_folium import st_folium 
import core.c_data_service as ds
import core.c_ui as ui
import time

# 1. 頁面設定
st.set_page_config(
    layout="wide", 
    page_title="台灣夜市風險地圖", 
    page_icon="🚦",
    initial_sidebar_state="expanded")

def main():
    # --- 樣式設定 ---
    st.markdown("""
    <style>
        .hero-metric-box {
            text-align: center;
            padding: 20px;
            background: #ffffff;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
            border: 1px solid #e2e8f0;
        }
        .action-title { font-size: 1.1rem; color: #334155; font-weight: bold; margin-bottom: 8px; }
        .action-desc { color: #64748b; font-size: 0.95rem; margin-bottom: 15px; line-height: 1.5; }
    </style>
    """, unsafe_allow_html=True)

    # --- 載入動畫 ---
    splash = st.empty()
    with splash.container():
        st.markdown("""
        <style>
            .splash-container { display: flex; flex-direction: column; justify-content: center; align-items: center; height: 70vh; background-color: #ffffff; animation: fadeIn 1s; }
            .loading-text { color: #444; font-family: "Microsoft JhengHei", sans-serif; margin-top: 20px; }
            .sub-text { color: #888; font-size: 0.9em; }
        </style>
        <div class="splash-container">
            <img src="https://media.giphy.com/media/l0HlOaQcLJ2hHpYcw/giphy.gif" width="150" style="border-radius: 10px;">
            <h2 class="loading-text">🚦 正在分析全台路況數據...</h2>
            <p class="sub-text">整合 <b>150 萬筆</b> 交通事故 x <b>300+個</b> 夜市位置 x 天氣資料</p>
            <div style="width: 300px; height: 4px; background: #eee; margin-top: 15px; border-radius: 2px;">
                <div style="width: 100%; height: 100%; background: #ef4444; animation: loading 2s infinite;"></div>
            </div>
            <style>@keyframes loading { 0% { width: 0%; } 50% { width: 70%; } 100% { width: 100%; } }</style>
        </div>
        """, unsafe_allow_html=True)
        time.sleep(2.5) 
    splash.empty()

    # 取得全台資料 (保留抓取邏輯)
    traffic_global = ds.get_taiwan_heatmap_data()
    df_market = ds.get_all_nightmarkets()

    # 呼叫側邊欄
    is_overview, target_market, layers = ui.render_sidebar(df_market)

    # =========================================================
    # 🌟 主體排版：縮排集中視覺 (控制整體最大寬度)
    # =========================================================
    spacer_left, col_main, spacer_right = st.columns([0.5, 9, 0.5])

    with col_main:
        # --- 區塊 1：Hero 視覺與大標題 ---
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("""
            <div style="text-align: center;">
                <h1 style='color: #0f172a; font-size: 3.2rem; font-weight: 900; margin-bottom: 0px;'>
                    夜市行人<span style='color: #ef4444;'>地獄</span>❗❓數據揭露的真相
                </h1>
            </div>
        """, unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

        # # 據儀表板
        # m1, m2, m3 = st.columns(3, gap="medium")
        # with m1:
        #     st.markdown("<div class='hero-metric-box'><div style='color: #64748b; font-size: 16px; font-weight:bold;'>涵蓋全台夜市</div><div style='color: #0f172a; font-size: 38px; font-weight: 900;'>300<span style='color:#3b82f6;'>+</span> 處</div></div>", unsafe_allow_html=True)
        # with m2:
        #     st.markdown("<div class='hero-metric-box'><div style='color: #64748b; font-size: 16px; font-weight:bold;'>分析交通事故</div><div style='color: #0f172a; font-size: 38px; font-weight: 900;'>150<span style='color:#3b82f6;'> 萬+</span> 件</div></div>", unsafe_allow_html=True)
        # with m3:
        #     st.markdown("<div class='hero-metric-box'><div style='color: #64748b; font-size: 16px; font-weight:bold;'>追蹤分析區間</div><div style='color: #0f172a; font-size: 38px; font-weight: 900;'>5<span style='color:#3b82f6;'>+</span> 年</div></div>", unsafe_allow_html=True)

        # st.markdown("<br>", unsafe_allow_html=True)

        # --- 區塊 2：核心痛點引言 (🌟 改動 1：滿版顯示) ---
        st.markdown("""
            <div style="background-color: rgba(254, 242, 242, 0.9); padding: 20px; border-radius: 12px; border: 1px solid #fecaca; text-align: left; margin-bottom: 30px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);">
                <div style="font-size: 1.15rem; color: #b91c1c; font-weight: bold; margin-bottom: 10px;">
                    「外媒直指臺灣街頭是行人地獄，我們的夜市真的安全嗎？」
                </div>
                <div style="color: #475569; font-size: 1rem; line-height: 1.7;">
                    根據統計，臺灣行人交通事故死亡率高達每十萬人 <b>1.56</b> 人，是鄰國日本的兩倍之多。而高達 <b>83.84%</b> 的國際旅客來臺必訪夜市。在人車混流、動線擁擠的環境中，過馬路不該是賭命！
                </div>
            </div>
        """, unsafe_allow_html=True)
        
        # --- 區塊 3：受眾價值卡片 (4 欄並排) ---
        st.markdown("<h3 style='text-align: left; color: #1e293b; margin-bottom: 20px;'>🎯 這套系統能為您做什麼？</h3>", unsafe_allow_html=True)
        
        c1, c2, c3, c4 = st.columns(4, gap="small")
        with c4:
            with st.container(border=True):
                st.markdown("<h4>🚶‍♂️ 夜市行人</h4>", unsafe_allow_html=True)
                st.write("掌握目前位置周邊的事故熱點，避開高風險路段，獲取最安全的友善步行路線建議。")
        with c3:
            with st.container(border=True):
                st.markdown("<h4>🏡 在地居民</h4>", unsafe_allow_html=True)
                st.write("比較居住區域的歷年交通趨勢，識別家門口的危險因子，精準規劃出低風險生活圈。")
        with c2:
            with st.container(border=True):
                st.markdown("<h4>🏪 夜市攤商</h4>", unsafe_allow_html=True)
                st.write("診斷特定夜市的交通環境風險，提早防範塞車與事故發生，保障客流與日常進出貨安全。")
        with c1:
            with st.container(border=True):
                st.markdown("<h4>🏛️ 地方政府</h4>", unsafe_allow_html=True)
                st.write("透過全台對標與 YoY 數據比較，科學化評估「人本交通」政策執行成效與未來改善空間。")
        # =========================================================
        # 🌟 改動 2：下半部改為「橫式排列」，逐層堆疊
        # =========================================================
        
        # --- 橫列一：數據解密 ---
        st.markdown("<h3 style='color: #1e293b; margin-bottom: 15px;'><span style='font-size: 1.4rem;'>🧭</span> 數據解密</h3>", unsafe_allow_html=True)
        row1_col1, row1_col2, row1_col3 = st.columns(3, gap="large")
        
        with row1_col1:
            with st.container(border=True):
                st.markdown("<div class='action-title'>🔥 哪裡最危險？</div><div class='action-desc'>巨觀掃描全台事故熱區，找出版圖盲區。</div>", unsafe_allow_html=True)
                if st.button("🗺️ 探索全台夜市總體檢", use_container_width=True): st.switch_page("pages/v_act1_all_accident.py")
        with row1_col2:
            with st.container(border=True):
                st.markdown("<div class='action-title'>🏙️ 我們的城市及格嗎？</div><div class='action-desc'>檢視跨縣市安全對標與年度進步率。</div>", unsafe_allow_html=True)
                if st.button("📊 縣市安全對標與趨勢", use_container_width=True): st.switch_page("pages/v_act1_city_accident.py")
        with row1_col3:
            with st.container(border=True):
                st.markdown("<div class='action-title'>🔍 為何會發生？</div><div class='action-desc'>深入單一夜市，探究天氣、時段與肇因特徵。</div>", unsafe_allow_html=True)
                if st.button("🤖 單一夜市 AI 深度診斷", use_container_width=True): st.switch_page("pages/v_act1_single_accident.py")

        st.markdown("<br>", unsafe_allow_html=True)


        # --- 橫列二：化數據為行動 ---
        st.markdown("<h3 style='color: #1e293b; margin-bottom: 15px;'><span style='font-size: 1.4rem;'>🛡️</span> 化數據為行動</h3>", unsafe_allow_html=True)
        row2_col1, row2_col2, row2_col3 = st.columns(3, gap="large")
        
        with row2_col1:
            with st.container(border=True):
                st.markdown("<div class='action-title'>🏛️ 政策監督</div><div class='action-desc'>「停讓行人」新法真的有用嗎？檢視修法前後事故變化。</div>", unsafe_allow_html=True)
                if st.button("⚖️ 政策成效即時監控", use_container_width=True): st.switch_page("pages/v_policy_impact.py") 
                # 加入 Tableau 歷史數據按鈕
                if st.button("📈 政策成效歷史數據 (Tableau)", use_container_width=True): st.switch_page("pages/v_policy_tableau.py") 
        with row2_col2:
            with st.container(border=True):
                st.markdown("<div class='action-title'>🚶 行人防護</div><div class='action-desc'>不只告訴您哪裡危險，更直接為您規劃避開熱點的安全路線。</div>", unsafe_allow_html=True)
                if st.button("🧭 友善步行導航路線", use_container_width=True): st.switch_page("pages/v_act3_avoid.py")
        with row2_col3:
            with st.container(border=True):
                st.markdown("<div class='action-title'>💡 智能決策</div><div class='action-desc'>結合 LLM 大語言模型，為您解答交通法規與安全疑難雜症。</div>", unsafe_allow_html=True)
                if st.button("💬 AI 交通小幫手", use_container_width=True): st.switch_page("pages/v_act6_chat.py")

        st.markdown("<br>", unsafe_allow_html=True)

        # # --- 橫列三：資料來源 ---
        # st.markdown("<h3 style='color: #1e293b; margin-bottom: 15px;'><span style='font-size: 1.4rem;'>📚</span> 資料來源</h3>", unsafe_allow_html=True)
        # st.info("""
        # * **交通事故資料**: 政府資料開放平台（2021–2024）
        # * **氣象即時資料**: 中央氣象署 CWA OpenData
        # * **氣象歷史資料**: 中央氣象署 CODiS
        # * **夜市空間資料**: Google Maps Places API
        # * **地理圖資底圖**: OpenStreetMap (OSM)
        # """)  
        # st.markdown("<br><br>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()