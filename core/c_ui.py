import streamlit as st
import folium
from folium.plugins import HeatMap, MarkerCluster
import pandas as pd
import altair as alt 
import time
from contextlib import contextmanager
import streamlit.components.v1 as components
import uuid

# ==========================================
# 1. 側邊欄 (Sidebar)
# 所有分頁的左側導航欄
# ==========================================
def render_sidebar(df_market):
    st.sidebar.markdown("### 🌐 語言切換 / Language")

    render_google_translator()
    # 側邊欄結構
    st.sidebar.markdown("## 數據揭密")
    st.sidebar.page_link("r_app.py",  label="首頁", icon="🏠")
    st.sidebar.page_link("pages/v_act1_all_accident.py", label="全台夜市事故總體檢", icon="🗺️")
    st.sidebar.page_link("pages/v_act1_city_accident.py", label="縣市安全對標與趨勢", icon="🏙️")
    st.sidebar.page_link("pages/v_act1_single_accident.py", label="單一夜市 AI 深度診斷", icon="🔍")
    st.sidebar.markdown("## 化數據為行動")
    st.sidebar.page_link("pages/v_act2_policy.py", label="夜市周遭 - 修法前後分析研究", icon="⚖️")
    st.sidebar.page_link("pages/v_act2_tableau.py", label="全國區域 - 修法前後分析研究", icon="📈") 
    st.sidebar.page_link("pages/v_act2_avoid.py", label="友善步行導航路線", icon="🧭")
    st.sidebar.markdown("### 持續開發中")
    st.sidebar.page_link("pages/v_act3_chat.py", label="AI交通小幫手", icon="💬")
    st.sidebar.page_link("pages/v_act3_policy_impact.py", label="政策成效初版", icon="⚖️")

    # 預設地圖圖層的開關狀態
    layers = {
        "traffic_heat": True,
        "night_market": True,
        "weather": False,
        "accidents": True}
    return True, None, layers

# 效能計時器上下文管理器
# 使用 with 語法包住一段程式碼，便可計算該區塊的執行時間，方便進行效能調校
@contextmanager
def page_timer():
    """
    保留此函式以防止報錯
    計算時間但不再 st.sidebar 中顯示內容
    """
    start_time = time.time()
    yield # 執行頁面主內容
    end_time = time.time()
    # 計算結果僅保留，不進行 UI 輸出
    _ = end_time - start_time
    
# ==========================================
# 地圖
# 依據傳入的參數決定要畫「全台總覽熱力圖」還是「單一夜市細節圖」
# ==========================================
def build_map(is_overview, target_market, layers, dynamic_zoom, radius_m, traffic_global, df_local, df_market, custom_tiles="CartoDB positron"):
    # 視角初始化
    # 如果是全台總覽，中心定在台灣中部；如果是看單一夜市，則將地圖中心綁定到該夜市經緯度
    if is_overview: 
        loc, zoom = [23.7, 120.95], 8
    elif target_market is not None: 
        loc = [target_market['lat'], target_market['lon']]
        # 接收 v_act1_single_accident 傳來的動態縮放值，若無則預設 16
        zoom = dynamic_zoom if dynamic_zoom is not None else 16
    else: 
        loc, zoom = [25.03, 121.56], 12
        
    # 將 tiles 改為使用傳入的 custom_tiles 變數
    # prefer_canvas=True：強制 Folium 使用 HTML5 Canvas 繪製點位，大幅提升繪製幾千個點的效能
    m = folium.Map(location=loc, zoom_start=zoom, tiles=custom_tiles, prefer_canvas=True)

    # [圖層 1] 全台交通熱力圖
    if layers.get('traffic_heat') and traffic_global:
        HeatMap(traffic_global, radius=15, blur=12, min_opacity=0.3).add_to(m)

    # [圖層 2] 夜市點位標示
    if layers.get('night_market'):
        fg_m = folium.FeatureGroup(name="夜市周圍邊界")
        if target_market is not None:
            # 針對單一夜市畫出星星 icon 與橘色分析範圍圓圈 (radius_m)
            folium.Marker([target_market['lat'], target_market['lon']], icon=folium.Icon(color='purple', icon='star', prefix='fa'), tooltip=target_market['MarketName']).add_to(fg_m)
            folium.Circle([target_market['lat'], target_market['lon']], radius=radius_m, color='orange', fill=True, fill_opacity=0.1).add_to(fg_m)
        else:
            # 總覽模式：畫出全台所有夜市的紫小圓點
            for _, r in df_market.iterrows():
                folium.CircleMarker([r['lat'], r['lon']], radius=3, color='purple', tooltip=r['MarketName']).add_to(fg_m)
        fg_m.add_to(m)

    # [圖層 3] 在地事故點位 (拆分為一般事故與死亡事故)
    if not is_overview and layers.get('accidents') and df_local is not None and not df_local.empty:
        df_death = df_local[df_local['death_count'] > 0]
        df_other = df_local[df_local['death_count'] == 0]

        # 將一般事故放入專屬圖層
        fg_other = folium.FeatureGroup(name="一般受傷事故")
        # 視覺意圖：效能防護機制
        # 如果單一區域事故 > 800 筆，為避免瀏覽器卡死，自動降級為熱力圖呈現；否則使用叢集點位
        if len(df_other) > 800:
            heat_data = [[r.latitude, r.longitude] for r in df_other.itertuples()]
            folium.plugins.HeatMap(heat_data, radius=12, blur=15, min_opacity=0.3).add_to(fg_other)
        else:
            # disableClusteringAtZoom=16: 當地圖放大到 level 16 時，強制散開所有群聚點位以利檢視
            cluster_other = MarkerCluster(maxClusterRadius=30, disableClusteringAtZoom=16).add_to(fg_other)
            for r in df_other.itertuples():
                i_count = getattr(r, 'injury_count', 0)
                color = 'blue' if i_count > 0 else 'black' # 有受傷標藍色，僅財損標黑色
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
                
                # CSS：使用客製化 HTML DivIcon 創造類似警示燈的紅色圓點
                # box-shadow 製造光暈效果；z_index_offset=1000 強制讓死亡事故疊加在所有一般事故之上，突出其嚴重性
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

# ==========================================
# 外國觀光客友善
# 透過插入Google Translate的 JS 腳本，達成多國語言翻譯
# ==========================================
def render_google_translator():
    container_id = f"google_translate_{uuid.uuid4().hex}"
    st.sidebar.markdown(f'<div id="{container_id}"></div>', unsafe_allow_html=True)
    st.sidebar.markdown("---")
    
    components.html(
        f"""
        <script>
        // 利用 window.parent 跨越 Streamlit iframe 的限制，將翻譯工具注入到最頂層視窗
        const parentWindow = window.parent;
        const parentDoc = parentWindow.document;

        if (!parentWindow.persistent_google_translate) {{
            parentWindow.persistent_google_translate = parentDoc.createElement('div');
            parentWindow.persistent_google_translate.id = 'persistent_google_translate';
            
            parentWindow.googleTranslateElementInit = function() {{
                new parentWindow.google.translate.TranslateElement({{
                    pageLanguage: 'zh-TW',
                    includedLanguages: 'zh-TW,en,ja,ko',
                    layout: parentWindow.google.translate.TranslateElement.InlineLayout.SIMPLE
                }}, 'persistent_google_translate'); 
            }};
            
            const script = parentDoc.createElement('script');
            script.id = 'google-translate-script';
            script.src = 'https://translate.google.com/translate_a/element.js?cb=googleTranslateElementInit';
            parentDoc.body.appendChild(script);
        }}

        // 定期檢查 DOM，把翻譯元件搬回 Streamlit Sidebar 中的指定位置
        let attempts = 0;
        const timer = setInterval(() => {{
            const newContainer = parentDoc.getElementById('{container_id}');
            if (newContainer && parentWindow.persistent_google_translate) {{
                newContainer.appendChild(parentWindow.persistent_google_translate);
                clearInterval(timer);
            }}
            attempts++;
            if (attempts > 50) clearInterval(timer);
        }}, 100);
        </script>
        """,
        height=0, width=0)

# ==========================================
# CSS：統整所有頁面的卡片、標題、KPI 樣式
# ==========================================
def load_custom_css():
    st.markdown("""
    <style>
        /* 共用：PDI 危險指數卡片 */
        /* hover 設定創造懸浮感 (transform: translateY)，提升質感與可點擊提示 */
        .pdi-card { padding: 18px; border-radius: 12px; box-shadow: 0 4px 10px rgba(0,0,0,0.15); transition: 0.2s; height: 100%; color: white; margin-bottom: 10px;}
        .pdi-card:hover { transform: translateY(-3px); box-shadow: 0 6px 15px rgba(0,0,0,0.25); }
        
        /* 共用：標題與區塊排版 */
        /* 使用主色調 #e11d48 (搶眼的玫瑰紅) 強調重點標題 */
        .title-highlight { color: #e11d48; font-weight: bold; }
        .section-title { font-size: 1.1rem; font-weight: 600; margin-bottom: 0.5rem; color: #333; }
        div[data-testid="stVerticalBlock"] > div { padding-bottom: 0rem; }
        
        /* 共用：KPI 數據方塊 (用於各縣市比較頁面) */
        /* 以淺灰底與圓角建構類似儀表板 (Dashboard) 的數據塊 */
        .kpi-box { background-color: #f8f9fa; padding: 10px; border-radius: 8px; text-align: center; border: 1px solid #e5e7eb; }
        .kpi-title { font-size: 13px; color: #6b7280; margin-bottom: 2px; }
        .kpi-value { font-size: 22px; font-weight: bold; color: #111827; }
        .kpi-delta { font-size: 12px; font-weight: bold; }
        /* 綠升紅降，符合投資/數據看板的直覺認知 */
        .delta-good { color: #10b981; }
        .delta-bad { color: #ef4444; }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 新增來自 market_tools.py 的 UI 元件
# ==========================================
def html_template(): 
    """回傳無縮排的 HTML 卡片模板字串"""
    # CSS：使用 clamp() 函數達成響應式 (RWD) 字體大小
    # 讓卡片在手機版與電腦版螢幕上都能保持最佳排版比例
    # white-space:nowrap 確保夜市名稱太長時不斷行並以 "..."
    return """
<a href="{url}" target="_blank" style="text-decoration:none;display:inline-block;">
<div class="pdi-card" style="width:clamp(180px,30vw,260px);padding:15px;border-radius:12px;background-color:#ffffff;box-shadow:0 4px 10px rgba(0,0,0,0.15);text-align:center;border:1px solid #eee;transition:transform 0.15s ease, box-shadow 0.15s ease;cursor:pointer;">
<div style="font-size:clamp(16px,2vw,22px);font-weight:bold;margin-bottom:8px;color:#333;">🥇 第 {rank} 名</div>
<div style="font-size:clamp(14px,2vw,20px);font-weight:bold;color:#333;margin-bottom:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{name}</div>
<div style="font-size:clamp(12px,1.8vw,18px);margin-bottom:4px;color:#333;">PDI：<b>{pdi}</b>（{level}）</div>
<div style="font-size:clamp(12px,1.5vw,16px);color:#555;">事故數：{count} 件</div>
</div>
</a>
"""

# CSS：危險分級漸層色塊
# 使用 linear-gradient ，從綠(安全)、黃(注意)、橘(危險)到紅(極危險)
def danger_color(pdi):
    """根據 PDI 分數回傳對應的 CSS 漸層背景顏色"""
    if pdi <= 10:
        return "linear-gradient(135deg, #81c784, #43a047)"  # 綠
    elif pdi <= 30:
        return "linear-gradient(135deg, #fff176, #fdd835)"  # 黃
    elif pdi <= 60:
        return "linear-gradient(135deg, #ffcc80, #ff7043)"  # 橘
    else:
        return "linear-gradient(135deg, #ff8a80, #e53935)"  # 紅

def pdi_divider(level):
    """根據危險等級渲染 Streamlit 水平分隔線 (hr)"""
    colors = {
        "安全": "linear-gradient(90deg, #43a047, #81c784 )",
        "注意": "linear-gradient(90deg, #fff59d, #fdd835 )",
        "危險": "linear-gradient(90deg, #ff7043, #ffcc80 )",
        "極危險": "linear-gradient(90deg, #e53935, #ff8a80 )"
    }

    st.markdown(f"""
    <hr style="
        border: 0;
        height: 5px;
        background: {colors.get(level, '#eee')};
        border-radius: 3px;
    ">
    """, unsafe_allow_html=True)