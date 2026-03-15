import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
import os
import redis
import pickle

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import core.c_data_service as ds
import core.c_ui as ui
from core.r_cache import REDIS_POOL

st.set_page_config(layout="wide", page_title="政策成效即時監控", page_icon="⚖️")

# 從 Redis 讀取全台總表並補齊時間/地理特徵
# 使用 @st.cache_data 將讀取的 DataFrame 暫存在 Streamlit 伺服器記憶體中，提升報表切換的流暢度
@st.cache_data(ttl=3600, show_spinner=False)
def get_real_policy_data():
    try:
        r = redis.Redis(connection_pool=REDIS_POOL)
        data = r.get("market:national_master_df")
        if data:
            df = pickle.loads(data)
            
            # 資料清洗：去重複與時間特徵抽取
            # 確保同一張事故不被重複計算
            if 'accident_id' in df.columns:
                df = df.drop_duplicates(subset=['accident_id'])
                
            df['accident_date'] = pd.to_datetime(df['accident_datetime'])
            df['year_quarter'] = df['accident_date'].dt.year.astype(str) + " Q" + df['accident_date'].dt.quarter.astype(str)
            df['city'] = df['nightmarket_city']
            
            # 確保完整時間欄位存在
            if 'Quarter' not in df.columns: df['Quarter'] = df['accident_date'].dt.quarter
            if 'Month' not in df.columns: df['Month'] = df['accident_date'].dt.month
            if 'Weekday' not in df.columns: df['Weekday'] = df['accident_date'].dt.weekday + 1
            if 'Hour' not in df.columns: df['Hour'] = df['accident_date'].dt.hour
            
            df = df[(df['city'].notna()) & (df['city'].astype(str).str.strip() != '') & (df['city'] != 'None')]
            
            region_map = {
                "臺北市": "北部", "新北市": "北部", "基隆市": "北部", "桃園市": "北部", "新竹市": "北部", "新竹縣": "北部", "宜蘭縣": "北部",
                "臺中市": "中部", "苗栗縣": "中部", "彰化縣": "中部", "南投縣": "中部", "雲林縣": "中部",
                "臺南市": "南部", "高雄市": "南部", "嘉義市": "南部", "嘉義縣": "南部", "屏東縣": "南部",
                "花蓮縣": "東部", "臺東縣": "東部", "澎湖縣": "離島", "金門縣": "離島", "連江縣": "離島"
            }
            df['region'] = df['city'].map(region_map).fillna("其他")
            return df
    except Exception as e:
        st.error(f"Redis 讀取失敗: {e}")
    return pd.DataFrame()

# 接收 Pandas 欄位，若名稱與目標縣市相符，則回傳帶有醒目紅底的 CSS 字串，實作資料表的動態高亮功能
def highlight_col(s, target):
    if s.name == target:
        return ['background-color: #fee2e2; color: #ef4444; font-weight: bold;'] * len(s)
    return [''] * len(s)

def main():
    df_market = ds.get_all_nightmarkets()
    ui.render_sidebar(df_market)
    
    st.markdown("""
    <style>
    div[data-testid="stVerticalBlock"] > div { padding-bottom: 0rem; }
    .methodology-box { background-color: #f8fafc; border-left: 4px solid #3b82f6; padding: 12px 16px; border-radius: 0 8px 8px 0; margin-bottom: 10px; font-size: 13px; color: #334155; line-height: 1.5; }
    .methodology-title { font-weight: bold; color: #0f172a; margin-bottom: 4px; font-size: 14px; }
    .metric-card { padding: 16px; border-radius: 8px; color: white; margin-bottom: 10px; }
    .metric-card.neutral { background: linear-gradient(135deg, #64748b 0%, #475569 100%); }
    .metric-card.danger { background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%); }
    .metric-card.safe { background: linear-gradient(135deg, #10b981 0%, #059669 100%); }
    .metric-title { font-size: 12px; opacity: 0.9; font-weight: bold; }
    .metric-city { font-size: 22px; font-weight: bold; margin-bottom: 2px; }
    .metric-value { font-size: 14px; background-color: rgba(255,255,255,0.2); display: inline-block; padding: 2px 8px; border-radius: 4px; }
    .section-title { font-size: 1.1rem; font-weight: 600; margin-bottom: 0.5rem; color: #333; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("<h2 style='margin-bottom: 5px;'>⚖️ 禮讓行人政策成效：修法前後分析研究</h2>", unsafe_allow_html=True)
    st.info("""
    💡 **數據判讀須知**：
    1. **過濾條件**：本頁僅計入夜市方圓 500 公尺核心區內涉及【行人（人與車）】之事故。
    2. **統計提示**：⚠️ **2026 Q1 數據目前僅更新至 1 月份**。修法後趨勢圖末端之波動係因 2026 年資料採計週期尚不完整所致。
    """)

    df_raw = get_real_policy_data()
    if df_raw.empty: return

    col_left, col_right = st.columns([1, 2.5], gap="large")

    with col_left:
        st.markdown('<div class="section-title" style="margin-bottom:8px;">切換分析視角</div>', unsafe_allow_html=True)
        global_metric = st.radio(
            "切換分析視角", 
            ["綜合危險指數 (PDI)", "行人事故件數", "死亡人數", "受傷人數"], 
            horizontal=True, 
            label_visibility="collapsed"
        )
        
        st.markdown("""
        <div style="background-color: #f1f5f9; border-left: 4px solid #94a3b8; padding: 16px; border-radius: 0 8px 8px 0; margin-bottom: 25px; margin-top: 10px;">
            <div style="font-weight: bold; color: #475569; margin-bottom: 8px; font-size: 14px;">💡 什麼是 PDI 危險指數？</div>
            <ul style="font-size: 13px; color: #475569; line-height: 1.7; margin-bottom: 0; padding-left: 20px;">
                <li><b>PDI 公式：</b> ((死亡×10 + 受傷×2) / 該區總事故數) × 1.5</li>
                <li><b>分母對齊：</b> 除以總件數標準化風險，消除規模誤差。</li>
                <li><b>判定：</b> 數值越高代表一旦發生事故「非死即傷」機率越高。</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('<div class="section-title">🗺️ 分析區域與縣市篩選</div>', unsafe_allow_html=True)
        with st.container(border=True):
            sel_region = st.selectbox("🗺️ 分析區域", ["全部區域"] + sorted(list(df_raw['region'].unique())))
            city_list = sorted(list(df_raw[df_raw['region'] == sel_region]['city'].unique())) if sel_region != "全部區域" else sorted(list(df_raw['city'].unique()))
            sel_city = st.selectbox("🏙️ 分析縣市", ["全部縣市"] + city_list)

        st.markdown('<div class="section-title" style="margin-top: 20px;">📅 分析時間篩選</div>', unsafe_allow_html=True)
        with st.container(border=True):
            l_c1, l_c2 = st.columns(2)
            year_opts = sorted([str(y) for y in df_raw['accident_date'].dt.year.unique()], reverse=True)
            with l_c1: sel_year = st.selectbox("年份", ["全部年份"] + year_opts)
            with l_c2: sel_q = st.selectbox("季度", ["全年", "第 1 季", "第 2 季", "第 3 季", "第 4 季"])
            
            l_c3, l_c4 = st.columns(2)
            with l_c3: sel_m = st.selectbox("月份", ["全部"] + [f"{i} 月" for i in range(1, 13)])
            week_map = {0: "全部", 1: "週一", 2: "週二", 3: "週三", 4: "週四", 5: "週五", 6: "週六", 7: "週日"}
            with l_c4: sel_w = st.selectbox("星期", list(week_map.values()))
            
            heat_mode = st.radio("時段", ["全部", "白天 (06-18)", "夜間 (18-06)"], horizontal=True)

    # 變數動態對應：根據上方單選框選擇的視角，自動切換 Pandas 要聚合的欄位與計算函數
    agg_col = 'pdi_score' if global_metric == "綜合危險指數 (PDI)" else 'accident_id' if global_metric == "行人事故件數" else 'death_count' if global_metric == "死亡人數" else 'injury_count'
    agg_func_city = 'mean' if global_metric == "綜合危險指數 (PDI)" else ('count' if global_metric == "行人事故件數" else 'sum')
    agg_func_trend = 'mean' if global_metric == "綜合危險指數 (PDI)" else ('count' if global_metric == "行人事故件數" else 'sum')

    # 基礎時間與區域篩選
    df_base = df_raw.copy()
    if sel_region != "全部區域": df_base = df_base[df_base['region'] == sel_region]
    
    if sel_q != "全年": df_base = df_base[df_base['Quarter'] == int(sel_q.split()[1])]
    if sel_m != "全部": df_base = df_base[df_base['Month'] == int(sel_m.split()[0])]
    if sel_w != "全部": df_base = df_base[df_base['Weekday'] == {v: k for k, v in week_map.items()}[sel_w]]
    if "白天" in heat_mode: df_base = df_base[(df_base['Hour'] >= 6) & (df_base['Hour'] < 18)]
    elif "夜間" in heat_mode: df_base = df_base[(df_base['Hour'] >= 18) | (df_base['Hour'] < 6)]
    
    # 排名比較資料 (不受「單一縣市」篩選影響，維持區域對標)
    df_compare = df_base.copy()

    # 趨勢與佔比資料 (會受到「單一縣市」篩選影響)
    df_trend_base = df_base.copy()
    if sel_city != "全部縣市": df_trend_base = df_trend_base[df_trend_base['city'] == sel_city]

    # 切分「修法前」與「修法後」的 DataFrame 
    # 若為「全部年份」，以政策上路的 2023-07-01 作為分水嶺；若選擇「單一年份」，則與該年份的「前一年」進行 YoY 對比
    if sel_year == "全部年份":
        date_policy_start, date_policy_end = pd.to_datetime("2023-07-01"), datetime.now()
        date_pre_start, date_pre_end = pd.to_datetime("2022-07-01"), pd.to_datetime("2023-06-30")
        
        df_pre_comp = df_compare[(df_compare['accident_date'] >= date_pre_start) & (df_compare['accident_date'] <= date_pre_end)]
        df_post_comp = df_compare[(df_compare['accident_date'] >= date_policy_start) & (df_compare['accident_date'] <= date_policy_end)]
        
        df_pre_trend = df_trend_base[(df_trend_base['accident_date'] >= date_pre_start) & (df_trend_base['accident_date'] <= date_pre_end)]
        df_post_trend = df_trend_base[(df_trend_base['accident_date'] >= date_policy_start) & (df_trend_base['accident_date'] <= date_policy_end)]
        df_curr_trend = df_trend_base.copy()
        
        pre_label, post_label = "修法前 (22/07-23/06)", "修法後 (23/07-至今)"
    else:
        target_year = int(sel_year)
        pre_year = target_year - 1
        
        df_pre_comp = df_compare[df_compare['accident_date'].dt.year == pre_year]
        df_post_comp = df_compare[df_compare['accident_date'].dt.year == target_year]
        
        df_pre_trend = df_trend_base[df_trend_base['accident_date'].dt.year == pre_year]
        df_post_trend = df_trend_base[df_trend_base['accident_date'].dt.year == target_year]
        df_curr_trend = df_trend_base[df_trend_base['accident_date'].dt.year == target_year]
        
        pre_label, post_label = f"{pre_year}年", f"{target_year}年"

    # 行人事故過濾
    df_pre_comp_ped = df_pre_comp[df_pre_comp['accident_type_major'] == '人與車']
    df_post_comp_ped = df_post_comp[df_post_comp['accident_type_major'] == '人與車']
    
    df_pre_trend_ped = df_pre_trend[df_pre_trend['accident_type_major'] == '人與車']
    df_post_trend_ped = df_post_trend[df_post_trend['accident_type_major'] == '人與車']
    df_curr_trend_ped = df_curr_trend[df_curr_trend['accident_type_major'] == '人與車']

    with col_right:
        st.markdown(f"<div style='font-size:17px; font-weight:bold; color:#1f2937; margin-bottom:12px;'>🚦 區域現況基準線 (以 {post_label} 絕對數值為準)</div>", unsafe_allow_html=True)
        
        # 這裡的 baseline_cities 固定使用目前區域的所有縣市，不受 sel_city 影響
        baseline_cities = pd.DataFrame({'city': city_list})
        
        if not df_post_comp_ped.empty:
            city_metrics = df_post_comp_ped.groupby('city').agg(val=(agg_col, agg_func_city), total_acc=('accident_id', 'count')).reset_index()
        else:
            city_metrics = pd.DataFrame(columns=['city', 'val', 'total_acc'])

        city_stats = pd.merge(baseline_cities, city_metrics, on='city', how='left').fillna({'val': 0, 'total_acc': 0})
        
        if not city_stats.empty:
            city_stats = city_stats.sort_values(by='val', ascending=False).reset_index(drop=True)
            city_stats.insert(0, '排名', range(1, len(city_stats) + 1))
            
            national_median_val = city_stats['val'].median()
            median_city_row = city_stats.iloc[(city_stats['val'] - national_median_val).abs().argsort()[:1]].iloc[0]
            worst_city = city_stats.iloc[0]
            best_city = city_stats.iloc[-1]
            
            val_fmt = "{:.2f}" if global_metric == "綜合危險指數 (PDI)" else "{:.0f}"
            unit = "分" if global_metric == "綜合危險指數 (PDI)" else "人" if "人數" in global_metric else "件"
            
            m1, m2, m3 = st.columns(3)
            with m1:
                st.markdown(f"""<div class="metric-card danger"><div class="metric-title">🚨 最高風險 (天花板)</div><div class="metric-city">{worst_city['city']}</div><div class="metric-value">{global_metric}: {val_fmt.format(worst_city['val'])} {unit} ({int(worst_city['total_acc'])}件事故)</div></div>""", unsafe_allow_html=True)
            with m2:
                st.markdown(f"""<div class="metric-card neutral"><div class="metric-title">🎯 區域中線基準 (Median)</div><div class="metric-city">{median_city_row['city']}</div><div class="metric-value">{global_metric}: {val_fmt.format(median_city_row['val'])} {unit} ({int(median_city_row['total_acc'])}件事故)</div></div>""", unsafe_allow_html=True)
            with m3:
                st.markdown(f"""<div class="metric-card safe"><div class="metric-title">🏆 最佳典範 (地板)</div><div class="metric-city">{best_city['city']}</div><div class="metric-value">{global_metric}: {val_fmt.format(best_city['val'])} {unit} ({int(best_city['total_acc'])}件事故)</div></div>""", unsafe_allow_html=True)

            # 加入選擇縣市時的動態標題註記
            target_city_info_curr = ""
            if sel_city != "全部縣市":
                target_rank_curr = city_stats[city_stats['city'] == sel_city]['排名'].values
                if len(target_rank_curr) > 0:
                    target_city_info_curr = f"&nbsp;&nbsp;<span style='color:#ef4444; font-size:14px;'>👉 鎖定目標：{sel_city} (排名第 {target_rank_curr[0]} 名)</span>"

            st.markdown(f"<div style='font-size:16px; font-weight:bold; color:#1f2937; margin-top:20px;'>📊 各縣市 {global_metric} 現況排名總表{target_city_info_curr}</div>", unsafe_allow_html=True)
            st.markdown("<div style='font-size:13px; color:#64748b; margin-bottom:10px;'>註：自動依據最新數值由高至低 (最危險至最安全) 排列，無資料之縣市以 0 顯示。可橫向滾動查看。</div>", unsafe_allow_html=True)
            
            with st.container(border=True):
                city_stats['val_str'] = city_stats['val'].apply(lambda x: val_fmt.format(x))
                city_stats['排名_str'] = city_stats['排名'].astype(str)
                
                df_transposed_curr = city_stats[['city', '排名_str', 'val_str']].set_index('city').T
                df_transposed_curr.index = ['排名', f'{global_metric}']
                
                # 套用 Pandas Styler 對指定縣市所在行進行紅底加粗高亮
                styled_curr = df_transposed_curr.style.apply(highlight_col, target=sel_city, axis=0) if sel_city != "全部縣市" else df_transposed_curr
                st.dataframe(styled_curr, use_container_width=True)

        st.markdown("<hr style='margin-top:10px; margin-bottom:15px;'>", unsafe_allow_html=True)

        col_trend, col_blind = st.columns(2, gap="large")

        with col_trend:
            st.markdown("<div style='font-size:16px; font-weight:bold; color:#1f2937;'>📈 肇因記錄轉換軌跡</div>", unsafe_allow_html=True)
            st.markdown("""
            <div class="methodology-box">
                <div class="methodology-title">💡 運算邏輯與判讀指南：</div>
                <ul>
                    <li><b>藍線 (整體趨勢)：</b>所有行人車禍的整體指標。</li>
                    <li><b>紅線 (駕駛過失)：</b>肇事主因為「駕駛者未禮讓行人」等過失的指標。</li>
                    <li><b>判讀核心：</b>紅線為政策直接約束對象。若修法後紅線的降幅明顯大於藍線，代表駕駛禮讓意識有實質提升。</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
            with st.container(border=True):
                # Plotly 雙線圖繪製
                # 將「整體行人事故」與「駕駛者主因」並列
                # 透過 add_vline 加入 2023 Q3 的垂直虛線，呈現政策實施前後的斷點
                trend_df = df_curr_trend_ped.groupby('year_quarter').agg(total_metric=(agg_col, agg_func_trend)).reset_index()
                driver_fault = df_curr_trend_ped[df_curr_trend_ped['cause_analysis_major'] == '駕駛者'].groupby('year_quarter')[agg_col].agg(agg_func_trend)
                trend_df['driver_fault_metric'] = trend_df['year_quarter'].map(driver_fault).fillna(0)

                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(x=trend_df['year_quarter'], y=trend_df['total_metric'], name='整體行人事故', mode='lines+markers', line=dict(color='#3b82f6', width=3, dash='dot')))
                fig_trend.add_trace(go.Scatter(x=trend_df['year_quarter'], y=trend_df['driver_fault_metric'], name='駕駛者過失', mode='lines+markers', line=dict(color='#ef4444', width=3)))
                
                if sel_year == "全部年份" or sel_year == "2023":
                    fig_trend.add_vline(x='2023 Q3', line_dash="dash", line_color="#8b5cf6", line_width=2)
                    fig_trend.add_annotation(x='2023 Q3', y=1.05, yref='paper', text="<b>🚨 新法上路</b>", showarrow=False, font=dict(size=12, color="#8b5cf6"), xanchor="left")
                
                fig_trend.update_layout(xaxis=dict(type='category', tickangle=-45), yaxis=dict(title=global_metric, rangemode='tozero'), legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1), height=280, margin=dict(l=0, r=0, t=10, b=0), plot_bgcolor='white', hovermode="x unified")
                st.plotly_chart(fig_trend, use_container_width=True)

        with col_blind:
            st.markdown("<div style='font-size:16px; font-weight:bold; color:#1f2937;'>🔍 時段盲區佔比</div>", unsafe_allow_html=True)
            st.markdown("""
            <div class="methodology-box">
                <div class="methodology-title">💡 運算邏輯與判讀指南：</div>
                <ul>
                    <li><b>計算方式：</b>該時段的「行人事故數量」佔「全類型交通事故」的百分比。</li>
                    <li><b>計算動態：</b>切換上方指標時，皆以該指標的「總和」進行分子/分母計算，精準呈現該時段的危險濃度。</li>
                    <li><b>判讀核心：</b>若時段佔比異常飆高，代表該時段的環境極度不利行人。</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
            with st.container(border=True):
                # 計算特定時段內的「行人事故」佔比
                # 需要建立一個以 24 小時為 index 的空白 DataFrame 作為基底並進行 Left Join，確保沒有發生事故的小時也能補 0，避免折線圖斷裂
                def get_hourly_ratio(data_df):
                    if data_df.empty: return pd.DataFrame({'Hour': range(24), 'ratio': 0})
                    
                    ratio_func = 'count' if global_metric == "行人事故件數" else 'sum'
                    ratio_col = 'accident_id' if global_metric == "行人事故件數" else agg_col
                    
                    total = data_df.groupby('Hour')[ratio_col].agg(ratio_func).reset_index(name='total_acc')
                    target = data_df[data_df['accident_type_major'] == '人與車'].groupby('Hour')[ratio_col].agg(ratio_func).reset_index(name='target_acc')
                    
                    merged = pd.merge(pd.DataFrame({'Hour': range(24)}), total, on='Hour', how='left')
                    merged = pd.merge(merged, target, on='Hour', how='left').fillna(0)
                    merged['ratio'] = np.where(merged['total_acc'] > 0, (merged['target_acc'] / merged['total_acc'] * 100).round(1), 0)
                    return merged

                hourly_pre = get_hourly_ratio(df_pre_trend)
                hourly_post = get_hourly_ratio(df_post_trend)

                fig_blind = go.Figure()
                if not df_pre_trend.empty: fig_blind.add_trace(go.Scatter(x=hourly_pre['Hour'], y=hourly_pre['ratio'], name=pre_label, mode='lines+markers', line=dict(color='#94a3b8', width=2, dash='dash'), marker=dict(size=6)))
                if not df_post_trend.empty: fig_blind.add_trace(go.Scatter(x=hourly_post['Hour'], y=hourly_post['ratio'], name=post_label, mode='lines+markers', line=dict(color='#ef4444', width=3), marker=dict(size=8)))
                
                # 標註夜市營運尖峰時段的黃色高亮區域
                fig_blind.add_vrect(x0=16.5, x1=23.5, fillcolor="#f59e0b", opacity=0.1, layer="below", line_width=0, annotation_text=" 夜市高峰", annotation_position="top left", annotation_font_color="#b45309")
                fig_blind.add_vrect(x0=-0.5, x1=1.5, fillcolor="#f59e0b", opacity=0.1, layer="below", line_width=0)
                
                fig_blind.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=0), xaxis=dict(title="小時", tickmode='linear', dtick=2), yaxis=dict(title="行人佔比 (%)", rangemode='tozero'), legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1), plot_bgcolor='white', hovermode="x unified")
                st.plotly_chart(fig_blind, use_container_width=True)

        st.markdown("<hr style='margin-top:15px; margin-bottom:15px;'>", unsafe_allow_html=True)
        
        # 先計算 DataFrame，以便提取最新排名資訊
        city_stats_yoy = []
        for city in baseline_cities['city'].unique():
            pre_data = df_pre_comp_ped[df_pre_comp_ped['city'] == city]
            post_data = df_post_comp_ped[df_post_comp_ped['city'] == city]
            
            val_pre = pre_data[agg_col].agg(agg_func_city) if not pre_data.empty else 0
            val_post = post_data[agg_col].agg(agg_func_city) if not post_data.empty else 0
            
            if global_metric == "綜合危險指數 (PDI)":
                if pd.isna(val_pre) or val_pre == 0: continue
                delta = ((val_post - val_pre) / val_pre * 100)
            else:
                delta = val_post - val_pre
                
            city_stats_yoy.append({"縣市": city, f"{pre_label}": val_pre, f"{post_label}": val_post, "變化指標": delta})
        
        df_city = pd.DataFrame(city_stats_yoy)
        
        target_city_info_yoy = ""
        if not df_city.empty:
            df_city = df_city.sort_values('變化指標', ascending=False).reset_index(drop=True)
            df_city.insert(0, '排名', range(1, len(df_city) + 1))
            
            # 加入選擇縣市時的動態標題註記
            if sel_city != "全部縣市":
                target_rank_yoy = df_city[df_city['縣市'] == sel_city]['排名'].values
                if len(target_rank_yoy) > 0:
                    target_city_info_yoy = f"&nbsp;&nbsp;<span style='color:#ef4444; font-size:14px;'>👉 鎖定目標：{sel_city} (排名第 {target_rank_yoy[0]} 名)</span>"

        st.markdown(f"<div style='font-size:16px; font-weight:bold; color:#1f2937;'>⚖️ 各縣市修法前後 YoY 改善幅度排行總表{target_city_info_yoy}</div>", unsafe_allow_html=True)
        st.markdown("""
        <div class="methodology-box">
            <div class="methodology-title">💡 YoY 變化量判讀差異：</div>
            <ul>
                <li>此圖表為「修法前後」或「今年對比去年」的變化量 (進步幅度)，排在越前面代表狀況<b>惡化最嚴重</b>。</li>
                <li>正值 (紅色) 代表「變危險 / 增加」；負值 (綠色) 代表「變安全 / 減少」。</li>
                <li>若指標選擇 PDI 則顯示改善率 (%)，其餘絕對指標顯示淨變化量以避免基數扭曲。</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        with st.container(border=True):
            if not df_city.empty:
                y_title = "YoY 改善率 (%)" if global_metric == "綜合危險指數 (PDI)" else "淨變化量 (增減數)"
                fmt_str = '{:+.1f}%' if global_metric == "綜合危險指數 (PDI)" else '{:+.0f}'
                
                df_city['val_str'] = df_city['變化指標'].apply(lambda x: fmt_str.format(x))
                df_city['排名_str'] = df_city['排名'].astype(str)
                
                df_transposed_yoy = df_city[['縣市', '排名_str', 'val_str']].set_index('縣市').T
                df_transposed_yoy.index = ['排名', y_title]
                
                # 套用 Pandas Styler 對 YoY 排行表進行紅底加粗高亮
                styled_yoy = df_transposed_yoy.style.apply(highlight_col, target=sel_city, axis=0) if sel_city != "全部縣市" else df_transposed_yoy
                st.dataframe(styled_yoy, use_container_width=True)
            else:
                st.info("無足夠對比資料產生 YoY 分析。")

if __name__ == "__main__":
    main()