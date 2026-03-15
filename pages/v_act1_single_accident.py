import streamlit as st
from streamlit_folium import st_folium
import pandas as pd
import numpy as np
import altair as alt
import plotly.express as px
import sys
import os
import re
import core.c_data_service as ds
import core.c_ui as ui
import redis
import pickle
from core.r_cache import REDIS_POOL
from dotenv import load_dotenv
from groq import Groq

load_dotenv() 
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dotenv_path = os.path.join(current_dir, '..', '.env')
load_dotenv(dotenv_path=parent_dotenv_path, override=True)

if not os.getenv("GROQ_API_KEY"):
    st.error("❌找不到 GROQ_API_KEY，請檢查 .env 檔案是否在正確位置。")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

st.set_page_config(layout="wide", page_title="單一夜市事故AI分析", page_icon="📊")

# 從 Redis 提取大範圍包裹並執行局部裁切
# 從 Redis 撈出資訊，再利用 Haversine 距離公式動態過濾出使用者指定半徑內的事故
@st.cache_data(ttl=86400, show_spinner=False)
def get_single_market_redis(lat, lon, radius_km):
    try:
        r = redis.Redis(connection_pool=REDIS_POOL)
        cache_key = f"traffic:nearby_v12:{lat:.4f}_{lon:.4f}_3.0_all_sample"
        data = r.get(cache_key)
        
        if data:
            result = pickle.loads(data)
            df = pd.DataFrame()
            if isinstance(result, tuple) and len(result) >= 1:
                df = result[0]
            elif isinstance(result, pd.DataFrame):
                df = result
            
            if not df.empty:
                distances = ds.haversine_distance(lat, lon, df['latitude'].values, df['longitude'].values)
                df_filtered = df[distances <= radius_km]
                return df_filtered
                
    except Exception as e:
        print(f"Redis 讀取失敗: {e}")
    return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_national_data_for_ranking():
    try:
        r = redis.Redis(connection_pool=REDIS_POOL)
        data = r.get("market:national_master_df")
        if data:
            df = pickle.loads(data)
            if 'accident_id' in df.columns:
                df = df.drop_duplicates(subset=['accident_id'])
            return df
    except: pass
    return pd.DataFrame()

def main():
    # 右上方卡片風格定義，使用 border-top製造 頂部邊框
    st.markdown("""
    <style>
    .section-title { font-size: 1.1rem; font-weight: 600; margin-bottom: 0.5rem; color: #333; margin-top: 15px; }
    div[data-testid="stVerticalBlock"] > div { padding-bottom: 0rem; }
    
    .top-card { 
        background-color: #ffffff; 
        padding: 15px 20px; 
        border-radius: 8px; 
        border: 1px solid #e2e8f0; 
        box-shadow: 0 2px 4px rgba(0,0,0,0.02); 
        height: 100%; 
        display: flex; 
        flex-direction: column; 
        justify-content: center;
        margin-bottom: 15px;
    }
    .top-card.danger { border-top: 4px solid #ef4444; }
    .top-card.neutral { border-top: 4px solid #64748b; }
    .top-card.highlight { border-top: 4px solid #3b82f6; }
    .top-card.safe { border-top: 4px solid #10b981; }
    
    .tc-title { font-size: 13px; font-weight: bold; color: #64748b; margin-bottom: 8px; }
    .tc-entity { font-size: 18px; font-weight: 900; color: #1e293b; margin-bottom: 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    
    .danger .tc-value { color: #ef4444; }
    .neutral .tc-value { color: #334155; }
    .highlight .tc-value { color: #3b82f6; }
    .safe .tc-value { color: #10b981; }
    .tc-value { font-size: 24px; font-weight: bold; margin-bottom: 8px; }
    .tc-value span { font-size: 14px; font-weight: normal; }
    
    .tc-sub-wrapper { display: flex; gap: 8px; flex-wrap: wrap; }
    .tc-sub { font-size: 12px; padding: 2px 8px; border-radius: 4px; display: inline-block; font-weight: bold; }
    .danger .tc-sub { background: #fee2e2; color: #ef4444; }
    .neutral .tc-sub { background: #f1f5f9; color: #475569; }
    .highlight .tc-sub { background: #dbeafe; color: #2563eb; }
    .safe .tc-sub { background: #d1fae5; color: #10b981; }

    .kpi-card { background: #ffffff; padding: 15px 10px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); text-align: center; border: 1px solid #e2e8f0; }
    .kpi-title { font-size: 13px; color: #64748b; font-weight: bold; margin-bottom: 4px; }
    .kpi-value { font-size: 24px; font-weight: 900; color: #1e293b; }
    </style>
    """, unsafe_allow_html=True)

    with ui.page_timer():
        df_market = ds.get_all_nightmarkets()
        
    st.session_state['show_accidents'] = True
    _, _, layers = ui.render_sidebar(df_market)

    st.markdown("""
        <h2 style="margin-bottom: 5px;">🔍 單一夜市 AI 深度診斷</h2>
    """, unsafe_allow_html=True)
    
    # 範圍定義與重疊差異的免責聲明
    st.info("💡 **資料範圍說明**：\n\n本頁提供單一夜市的**深度與大範圍環境探索**。您可以透過左側滑桿自訂分析半徑（500m ~ 3km）。\n\n*註：為避免相鄰夜市的事故重複計算，其他頁面（全台總表、縣市對標）的總數與排名，皆採用嚴格的「500m 去重複核心區」計算。因此當您拉大本頁半徑，或加總多個夜市的數值時，將因包含重疊區域及外圍幹道，而大於排行榜之淨總數。*")
    st.markdown("<hr style='margin-top: 5px; margin-bottom: 20px;'>", unsafe_allow_html=True)

    col_left, col_right = st.columns([1, 2.5], gap="large")

    with col_left:
        st.markdown('<div class="section-title" style="margin-top: 0;">📍 選擇分析目標</div>', unsafe_allow_html=True)
        search_mode = st.radio("尋找方式：", ["🔍 直接關鍵字搜尋", "🗺️ 區域層層篩選"], horizontal=True, label_visibility="collapsed")
        
        def_market = "士林夜市"
        if search_mode == "🔍 直接關鍵字搜尋":
            all_markets = sorted(df_market['MarketName'].dropna().unique())
            sel_market = st.selectbox("請輸入或選擇夜市名稱", all_markets, index=all_markets.index(def_market) if def_market in all_markets else 0)
        else:
            with st.container(border=True):
                def_region, def_city, def_dist = "北部", "臺北市", "士林區"
                region_opts = sorted(df_market['Region'].dropna().unique())
                sel_region = st.selectbox("區域", region_opts, index=region_opts.index(def_region) if def_region in region_opts else 0)
                
                city_opts = sorted(df_market[df_market['Region'] == sel_region]['City'].dropna().unique())
                sel_city = st.selectbox("縣市", city_opts, index=city_opts.index(def_city) if def_city in city_opts else 0)
                
                dist_opts = sorted(df_market[(df_market['Region'] == sel_region) & (df_market['City'] == sel_city)]['AdminDistrict'].dropna().unique())
                sel_dist = st.selectbox("行政區", dist_opts, index=dist_opts.index(def_dist) if def_dist in dist_opts else 0)

                m_opts = sorted(df_market[(df_market['Region'] == sel_region) & (df_market['City'] == sel_city) & (df_market['AdminDistrict'] == sel_dist)]['MarketName'].dropna().unique())
                if not m_opts:
                    st.warning("此區域無夜市")
                    st.stop()
                sel_market = st.selectbox("夜市", m_opts, index=m_opts.index(def_market) if def_market in m_opts else 0)
        
        target_market = df_market[df_market['MarketName'] == sel_market].iloc[0]

        # 清除跨夜市殘留的 AI 報告
        # 利用 Streamlit 的 session_state 紀錄上一次選擇的夜市。若發現夜市更換了，則清空舊的 AI 報告內容
        if "last_market" not in st.session_state:
            st.session_state.last_market = sel_market
        if st.session_state.last_market != sel_market:
            if "ai_report_text" in st.session_state:
                st.session_state.ai_report_text = ""
            st.session_state.last_market = sel_market

        st.markdown('<div class="section-title">🎯 分析範圍與圖層</div>', unsafe_allow_html=True)
        with st.container(border=True):
            # 預設值保持 500m，呼應其他頁面的設定
            radius_m = st.slider("選擇分析半徑 (公尺)", min_value=500, max_value=3000, step=500, value=500)
            heat_mode = st.radio("事故圖層時段", ["🌍 全部", "☀️ 白天 (06-18)", "🌙 夜間 (18-06)"], horizontal=True)

        radius_km = radius_m / 1000.0
        with st.spinner(f"正在載入 {sel_market} 周邊資料..."):
            df_raw = get_single_market_redis(target_market['lat'], target_market['lon'], radius_km)

        if df_raw.empty:
            st.error("⚠️ 該區域無事故資料，請嘗試擴大半徑。")
            st.stop()

        st.markdown('<div class="section-title">📅 分析時間篩選</div>', unsafe_allow_html=True)
        with st.container(border=True):
            df_raw['accident_datetime'] = pd.to_datetime(df_raw['accident_datetime'])
            df_raw['Year'] = df_raw['accident_datetime'].dt.year
            df_raw['Quarter'] = df_raw['accident_datetime'].dt.quarter
            df_raw['Month'] = df_raw['accident_datetime'].dt.month
            df_raw['Weekday'] = df_raw['accident_datetime'].dt.weekday + 1

            yrs = sorted(df_raw['Year'].dropna().unique(), reverse=True)
            sel_y = st.selectbox("年份", ["全部年份"] + [str(int(y)) for y in yrs])
            sel_q = st.selectbox("季度", ["全年", "第 1 季", "第 2 季", "第 3 季", "第 4 季"])
            sel_m = st.selectbox("月份", ["全部"] + [f"{m} 月" for m in range(1, 13)])
            week_map = {0: "全部", 1: "週一", 2: "週二", 3: "週三", 4: "週四", 5: "週五", 6: "週六", 7: "週日"}
            sel_w = st.selectbox("星期幾", list(week_map.values()))

    df_filtered = df_raw.copy()
    if sel_y != "全部年份": df_filtered = df_filtered[df_filtered['Year'] == int(sel_y)]
    if sel_q != "全年": df_filtered = df_filtered[df_filtered['Quarter'] == int(sel_q.split()[1])]
    if sel_m != "全部": df_filtered = df_filtered[df_filtered['Month'] == int(sel_m.split()[0])]
    if sel_w != "全部": df_filtered = df_filtered[df_filtered['Weekday'] == {v: k for k, v in week_map.items()}[sel_w]]
    if "白天" in heat_mode: df_filtered = df_filtered[(df_filtered['Hour'] >= 6) & (df_filtered['Hour'] < 18)]
    elif "夜間" in heat_mode: df_filtered = df_filtered[(df_filtered['Hour'] >= 18) | (df_filtered['Hour'] < 6)]

    if df_filtered.empty:
        with col_right:
            st.error("⚠️ 在您設定的篩選條件下，沒有發生任何事故記錄。")
        st.stop()

    # 此處為「拉桿範圍」的自訂數值
    total_count = len(df_filtered)
    dead_count = int(df_filtered['death_count'].sum())
    hurt_count = int(df_filtered['injury_count'].sum())
    
    if 'pdi_score' in df_filtered.columns:
        local_pdi = df_filtered['pdi_score'].mean()
    else:
        df_filtered['weight'] = np.where((df_filtered['Hour'] >= 17) | (df_filtered['Hour'] == 0), 1.5, 1.0)
        df_filtered['pdi_score'] = (df_filtered['death_count'] * 10 + df_filtered['injury_count'] * 2) * df_filtered['weight']
        local_pdi = df_filtered['pdi_score'].mean() if total_count > 0 else 0

    # 天氣與路面條件特徵擷取
    # 原始資料中的 weather_condition 是自由填寫的文字(如「雨」、「小雨」)。
    # 這裡利用 apply + lambda 搭配關鍵字比對 (in) 轉化為 0 或 1 的二元特徵 (is_rain, is_wet)，方便後續繪製雷達圖
    df_radar = df_filtered.copy()
    df_radar['weather_condition'] = df_radar['weather_condition'].fillna('')
    df_radar['light_condition'] = df_radar['light_condition'].fillna('')
    df_radar['road_surface_condition'] = df_radar['road_surface_condition'].fillna('')
    
    df_radar["is_rain"] = df_radar["weather_condition"].apply(lambda w: 1 if "雨" in w else 0)
    df_radar["is_dark"] = df_radar["light_condition"].apply(lambda x: 1 if any(k in x for k in ["暗", "夜", "未開啟", "無照明"]) else 0)
    df_radar["is_wet"] = df_radar["road_surface_condition"].apply(lambda r: 1 if any(k in r for k in ["濕", "積水"]) else 0)
    
    rain_ratio = df_radar["is_rain"].mean() * 100 if total_count > 0 else 0
    dark_ratio = df_radar["is_dark"].mean() * 100 if total_count > 0 else 0
    wet_ratio = df_radar["is_wet"].mean() * 100 if total_count > 0 else 0

    with col_left:
        st.markdown("<hr style='margin-top:15px; margin-bottom:15px;'>", unsafe_allow_html=True)
        st.markdown("<div class='section-title' style='margin-top:0;'>📈 歷年季度事故趨勢</div>", unsafe_allow_html=True)
        
        # Altair 雙軸圖表 (Dual-axis chart)
        # 用長條圖 (mark_bar) 表示死亡人數，折線圖 (mark_line) 表示總事故數與受傷人數
        # 利用 resolve_scale(y='independent') 讓兩者的 Y 軸互不影響，同時呈現在同一個畫布上
        df_t = df_filtered.copy()
        df_t['年季'] = df_t['accident_datetime'].dt.year.astype(str) + " Q" + df_t['accident_datetime'].dt.quarter.astype(str)
        trend_grp = df_t.groupby('年季').agg(
            事故總數=('accident_id', 'count'), 
            受傷人數=('injury_count', 'sum'), 
            死亡人數=('death_count', 'sum')
        ).reset_index()
        
        if not trend_grp.empty:
            bar = alt.Chart(trend_grp).mark_bar(opacity=0.4, color='#dc2626', size=15).encode(
                x=alt.X('年季:N', title=None, axis=alt.Axis(labelAngle=-45)),
                y=alt.Y('死亡人數:Q', title='死亡人數', axis=alt.Axis(orient='right', grid=False, titleColor='#dc2626', labelColor='#dc2626')),
                tooltip=['年季', '死亡人數']
            )
            trend_m = trend_grp.melt(id_vars=['年季'], value_vars=['事故總數', '受傷人數'], var_name='類別', value_name='數量')
            line = alt.Chart(trend_m).mark_line(point=True, strokeWidth=3).encode(
                x=alt.X('年季:N', title=None), 
                y=alt.Y('數量:Q', title='件數/受傷人數', axis=alt.Axis(grid=True)), 
                color=alt.Color('類別:N', scale=alt.Scale(domain=['事故總數', '受傷人數'], range=['#3b82f6', '#f59e0b']), legend=alt.Legend(orient="top", title=None)),
                tooltip=['年季', '類別', '數量']
            )
            dual_chart = alt.layer(bar, line).resolve_scale(y='independent').properties(height=260)
            st.altair_chart(dual_chart, use_container_width=True)
        else:
            st.info("無趨勢數據")

    with col_right:
        # 取得全國 500m 標準排行榜資料，用於頂部四張卡片
        df_nat = get_national_data_for_ranking()
        
        nat_rank_str, city_rank_str = "-", "-"
        nat_avg_pdi = 0
        worst_nm, best_nm = "-", "-"
        worst_pdi, best_pdi = 0, 0
        worst_cnt, best_cnt = 0, 0
        curr_pdi_base, curr_cnt_base = 0, 0
        
        if not df_nat.empty:
            df_nat_filt = df_nat.copy()
            if sel_y != "全部年份": df_nat_filt = df_nat_filt[df_nat_filt['Year'] == int(sel_y)]
            if sel_q != "全年": df_nat_filt = df_nat_filt[df_nat_filt['Quarter'] == int(sel_q.split()[1])]
            if sel_m != "全部": df_nat_filt = df_nat_filt[df_nat_filt['Month'] == int(sel_m.split()[0])]
            if sel_w != "全部": df_nat_filt = df_nat_filt[df_nat_filt['Weekday'] == {v: k for k, v in week_map.items()}[sel_w]]
            if "白天" in heat_mode: df_nat_filt = df_nat_filt[(df_nat_filt['Hour'] >= 6) & (df_nat_filt['Hour'] < 18)]
            elif "夜間" in heat_mode: df_nat_filt = df_nat_filt[(df_nat_filt['Hour'] >= 18) | (df_nat_filt['Hour'] < 6)]
            
            rank_df = df_nat_filt.groupby(['nightmarket_city', 'nightmarket_name']).agg(
                pdi_mean=('pdi_score', 'mean'),
                acc_count=('accident_id', 'count')
            ).reset_index()
            
            if len(rank_df) > 0:
                nat_avg_pdi = rank_df['pdi_mean'].mean()
                rank_df['nat_rank'] = rank_df['pdi_mean'].rank(ascending=False, method='min')
                
                target_city_name = target_market['City'].replace('台', '臺')
                city_df = rank_df[rank_df['nightmarket_city'] == target_city_name].copy()
                
                if not city_df.empty:
                    city_df['city_rank'] = city_df['pdi_mean'].rank(ascending=False, method='min')
                    
                    worst_row = city_df.loc[city_df['pdi_mean'].idxmax()]
                    best_row = city_df.loc[city_df['pdi_mean'].idxmin()]
                    worst_nm, worst_pdi, worst_cnt = worst_row['nightmarket_name'], worst_row['pdi_mean'], worst_row['acc_count']
                    best_nm, best_pdi, best_cnt = best_row['nightmarket_name'], best_row['pdi_mean'], best_row['acc_count']
                    
                    # 抓取「當前夜市」在 500m 基準下的分數與件數
                    curr_row = city_df[city_df['nightmarket_name'] == sel_market]
                    if not curr_row.empty:
                        nat_rank_str = f"第 {int(curr_row.iloc[0]['nat_rank'])} 名"
                        city_rank_str = f"第 {int(curr_row.iloc[0]['city_rank'])} 名"
                        curr_pdi_base = curr_row.iloc[0]['pdi_mean']
                        curr_cnt_base = curr_row.iloc[0]['acc_count']

        st.markdown(f"<div style='font-size:16px; font-weight:bold; color:#1f2937; margin-bottom:10px;'>🚦 {target_market['City']} 與全國安全基準對標 (依據 500m 核心區標準)</div>", unsafe_allow_html=True)
        
        # 頂部卡片全面強制顯示「500m 排行基準」，確保基準一致性
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"""<div class='top-card danger'>
                <div class='tc-title'>🚨 縣市最高風險 (天花板)</div>
                <div class='tc-entity'>{worst_nm}</div>
                <div class='tc-value'>PDI平均 {worst_pdi:.1f} <span>分</span></div>
                <div class='tc-sub-wrapper'><span class='tc-sub' style='background:transparent; color:#ef4444; padding:0;'></span></div>
            </div>""", unsafe_allow_html=True)
        with c2:
            st.markdown(f"""<div class='top-card neutral'>
                <div class='tc-title'>🇹🇼 全國基準線</div>
                <div class='tc-entity'>全國夜市平均</div>
                <div class='tc-value'>PDI平均 {nat_avg_pdi:.1f} <span>分</span></div>
                <div class='tc-sub-wrapper'><span class='tc-sub'>對標基準點</span></div>
            </div>""", unsafe_allow_html=True)
        with c3:
            st.markdown(f"""<div class='top-card highlight'>
                <div class='tc-title'>🎯 當前鎖定目標</div>
                <div class='tc-entity'>{sel_market}</div>
                <div class='tc-value'>PDI平均 {curr_pdi_base:.1f} <span>分</span></div>
                <div class='tc-sub-wrapper'>
                    <span class='tc-sub'>全國 {nat_rank_str}</span> 
                    <span class='tc-sub'>縣市 {city_rank_str}</span>
                    <span class='tc-sub' style='background:transparent; color:#3b82f6; padding:0; margin-top:2px;'></span>
                </div>
            </div>""", unsafe_allow_html=True)
        with c4:
            st.markdown(f"""<div class='top-card safe'>
                <div class='tc-title'>🏆 縣市最佳典範 (地板)</div>
                <div class='tc-entity'>{best_nm}</div>
                <div class='tc-value'>PDI平均 {best_pdi:.1f} <span>分</span></div>
                <div class='tc-sub-wrapper'><span class='tc-sub' style='background:transparent; color:#10b981; padding:0;'></span></div>
            </div>""", unsafe_allow_html=True)

        st.markdown(f"<h4 style='margin-top:0; color:#1e293b; margin-bottom:10px;'>🎯 {sel_market} 自訂周邊 {radius_m} 公尺安全體檢</h4>", unsafe_allow_html=True)
        
        # 這裡下方開始的 KPI 與圖表，皆聯動左側拉桿的 custom 範圍
        k1, k2, k3, k4, k5 = st.columns(5)
        with k1:
            st.markdown(f"<div class='kpi-card' style='border-top: 4px solid #3b82f6;'><div class='kpi-title'>📌 事故總數</div><div class='kpi-value'>{total_count:,} <span style='font-size:14px; font-weight:normal;'>件</span></div></div>", unsafe_allow_html=True)
        with k2:
            st.markdown(f"<div class='kpi-card' style='border-top: 4px solid #8b5cf6;'><div class='kpi-title'>🚦 綜合 PDI</div><div class='kpi-value'>{local_pdi:.1f} <span style='font-size:14px; font-weight:normal;'>分</span></div></div>", unsafe_allow_html=True)
        with k3:
            st.markdown(f"<div class='kpi-card' style='border-top: 4px solid #ef4444;'><div class='kpi-title'>💀 死亡人數</div><div class='kpi-value'>{dead_count:,} <span style='font-size:14px; font-weight:normal;'>人</span></div></div>", unsafe_allow_html=True)
        with k4:
            st.markdown(f"<div class='kpi-card' style='border-top: 4px solid #f59e0b;'><div class='kpi-title'>🚑 受傷人數</div><div class='kpi-value'>{hurt_count:,} <span style='font-size:14px; font-weight:normal;'>人</span></div></div>", unsafe_allow_html=True)
        with k5:
            st.markdown(f"<div class='kpi-card' style='border-top: 4px solid #06b6d4;'><div class='kpi-title'>🌧️ 雨天比例</div><div class='kpi-value'>{rain_ratio:.1f} <span style='font-size:14px; font-weight:normal;'>%</span></div></div>", unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)

        col_map, col_ai = st.columns([1.8, 1], gap="medium")
        with col_map:
            # 效能保護降級機制
            # 若點位過多 (超過 1000 個點)，Folium 地圖會讓瀏覽器卡死
            # 因此這裡優先保留所有死亡事故點位，一般事故則隨機抽樣 1500 點進行渲染地圖，平衡視覺豐富度與網頁效能
            df_for_map = df_filtered.copy()
            if len(df_for_map) > 1000:
                df_death_map = df_for_map[df_for_map['death_count'] > 0]
                df_other_map = df_for_map[df_for_map['death_count'] == 0]
                if len(df_other_map) > 1500: df_other_map = df_other_map.sample(n=1500, random_state=42)
                df_for_map = pd.concat([df_death_map, df_other_map])

            d_zoom = 16 if radius_m <= 500 else 15 if radius_m <= 1000 else 14 if radius_m <= 2000 else 13
            m = ui.build_map(False, target_market, layers, d_zoom, radius_m, None, df_for_map, df_market, custom_tiles="OpenStreetMap")
            st_folium(m, height=400, use_container_width=True, returned_objects=[])

        with col_ai:
            st.markdown("""
                <div style="background-color: #f8fafc; padding: 15px; border-radius: 12px; border: 1px solid #cbd5e1; height:100%;">
                    <div style="font-size:16px; font-weight:bold; color:#1f2937; margin-bottom:8px;">🤖 專屬 AI 深度分析</div>
                    <div style="font-size:13px; color:#475569; margin-bottom:15px; line-height:1.5;">綜合地圖熱點、時段與環境變數，產生防護建議。</div>
            """, unsafe_allow_html=True)
            
            top_cause_str = df_filtered['primary_cause'].value_counts().idxmax() if not df_filtered.empty else "未知"
            peak_hour_str = df_filtered.groupby('Hour').size().idxmax() if not df_filtered.empty else "未知"
            
            df_death = df_filtered[df_filtered['death_count'] > 0]
            if not df_death.empty:
                r_lat, r_lon = df_death.groupby(['latitude', 'longitude']).size().idxmax()
                risky_loc = f"https://www.google.com/maps/search/?api=1&query={r_lat},{r_lon} (曾發生死亡事故)"
            elif not df_filtered.empty:
                r_lat, r_lon = df_filtered.groupby(['latitude', 'longitude']).size().idxmax()
                risky_loc = f"https://www.google.com/maps/search/?api=1&query={r_lat},{r_lon} (高頻事故熱點)"
            else: 
                risky_loc = "無明顯熱點"

            if st.button("✨ 立即生成分析報告", type="primary", use_container_width=True):
                with st.spinner("AI 正在解讀地圖與數據..."):
                    st.session_state.ai_report_text = get_ai_analysis(
                        target_market['MarketName'], total_count, local_pdi, dead_count, hurt_count, 
                        top_cause_str, peak_hour_str, rain_ratio, dark_ratio, wet_ratio, risky_loc
                    )
            
            if st.session_state.get("ai_report_text"):
                st.markdown(f"<div style='margin-top:15px; font-size:13.5px; color:#334155; line-height:1.7;'>{st.session_state.ai_report_text}</div>", unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)

        # 為各種維度圖表之繪製
        st.markdown("<hr style='margin-top:20px; margin-bottom:20px;'>", unsafe_allow_html=True)
        st.markdown("<h4 style='margin-bottom: 15px; color:#1e293b;'>📊 多維度特徵剖析</h4>", unsafe_allow_html=True)

        chart_h = 240
        chart_cols = st.columns(4)
        
        with chart_cols[0]:
            # Plotly 雷達圖 (Polar Line Chart)
            # 利用雷達圖呈現天氣 (雨天)、光線 (光線不佳)、路面 (濕滑) 三種環境變數的佔比情形，幫助直觀判斷環境劣勢
            st.markdown("<div style='font-size:14px; font-weight:bold; color:#475569; text-align:center;'>🕸️ 環境風險雷達圖</div>", unsafe_allow_html=True)
            risk_df = pd.DataFrame({"risk": ["雨天", "光線不佳", "濕滑路面"], "value": [rain_ratio, dark_ratio, wet_ratio]})
            fig = px.line_polar(risk_df, r="value", theta="risk", line_close=True, markers=True, range_r=[0, max(risk_df["value"].max() * 1.2, 10)])
            fig.update_traces(fill="toself", marker=dict(color="#8E44AD", size=4), line=dict(color="#8E44AD"))
            fig.update_layout(polar=dict(radialaxis=dict(visible=False), angularaxis=dict(tickfont=dict(size=12))), margin=dict(l=15, r=15, t=10, b=10), height=chart_h, paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)

        with chart_cols[1]:
            st.markdown("<div style='font-size:14px; font-weight:bold; color:#475569; text-align:center;'>🔍 Top 5 肇因</div>", unsafe_allow_html=True)
            if 'primary_cause' in df_filtered.columns:
                df_cause = df_filtered['primary_cause'].value_counts().head(5).reset_index()
                df_cause.columns = ['肇因', '件數']
                bar_c = alt.Chart(df_cause).mark_bar(color="#b91d47", cornerRadiusEnd=4).encode(
                    x=alt.X('件數:Q', axis=None),
                    y=alt.Y('肇因:N', sort='-x', axis=alt.Axis(labels=True, title=None, labelLimit=70, labelFontSize=11)),
                    tooltip=['肇因', '件數']
                )
                text_c = bar_c.mark_text(align='left', dx=2, fontWeight='bold', color='black', fontSize=10).encode(text='件數:Q')
                st.altair_chart((bar_c + text_c).properties(height=chart_h), use_container_width=True)

        with chart_cols[2]:
            st.markdown("<div style='font-size:14px; font-weight:bold; color:#475569; text-align:center;'>🌙 24H 事故熱力</div>", unsafe_allow_html=True)
            df_h = df_filtered.groupby('Hour').size().reset_index(name='n')
            h_chart = alt.Chart(df_h).mark_area(color='lightblue', line={'color':'#2563eb'}, opacity=0.6).encode(
                x=alt.X('Hour:O', title=None, axis=alt.Axis(labelAngle=0, labelFontSize=10, values=[0,6,12,18,23])), 
                y=alt.Y('n:Q', title=None, axis=alt.Axis(labels=False))
            )
            st.altair_chart(h_chart.properties(height=chart_h), use_container_width=True)

        with chart_cols[3]:
            # Altair 堆疊圖
            # 以時段(白天/夜間)作為 X 軸
            st.markdown("<div style='font-size:14px; font-weight:bold; color:#475569; text-align:center;'>🌤️ 天氣佔比</div>", unsafe_allow_html=True)
            df_w = df_filtered.copy()
            df_w['天氣'] = df_w['weather_condition'].apply(lambda w: '雨天' if '雨' in str(w) else '晴天' if '晴' in str(w) else '陰天' if '陰' in str(w) else '其他')
            df_w['時段'] = df_w['Hour'].apply(lambda h: '白天' if 6 <= h < 18 else '夜間')
            chart_w_df = df_w.groupby(['時段', '天氣']).size().reset_index(name='件數')
            base_bar = alt.Chart(chart_w_df).encode(
                x=alt.X('時段:N', title=None, axis=alt.Axis(labelAngle=0, labelFontSize=11)), 
                y=alt.Y('件數:Q', title=None, axis=alt.Axis(labels=False)), 
                color=alt.Color('天氣:N', scale=alt.Scale(domain=['晴天', '陰天', '雨天', '其他'], range=['#fcd34d', '#94a3b8', '#3b82f6', '#cbd5e1']), legend=alt.Legend(orient="bottom", title=None, labelFontSize=10))
            )
            text_w = base_bar.mark_text(dy=8, color='black', fontWeight='bold', size=11).encode(y=alt.Y('件數:Q', stack='zero'), text=alt.condition(alt.datum.件數 > 0, alt.Text('件數:Q'), alt.value('')))
            st.altair_chart((base_bar.mark_bar() + text_w).properties(height=chart_h), use_container_width=True)

        st.markdown("<hr style='margin-top:20px; margin-bottom:20px;'>", unsafe_allow_html=True)
        st.markdown("<h4 style='margin-bottom: 10px; color:#1e293b;'>📅 歷年安全指標與排名變化</h4>", unsafe_allow_html=True)
        
        if not df_nat.empty:
            df_nat_hist = df_nat.copy()
            if sel_q != "全年": df_nat_hist = df_nat_hist[df_nat_hist['Quarter'] == int(sel_q.split()[1])]
            if sel_m != "全部": df_nat_hist = df_nat_hist[df_nat_hist['Month'] == int(sel_m.split()[0])]
            if sel_w != "全部": df_nat_hist = df_nat_hist[df_nat_hist['Weekday'] == {v: k for k, v in week_map.items()}[sel_w]]
            if "白天" in heat_mode: df_nat_hist = df_nat_hist[(df_nat_hist['Hour'] >= 6) & (df_nat_hist['Hour'] < 18)]
            elif "夜間" in heat_mode: df_nat_hist = df_nat_hist[(df_nat_hist['Hour'] >= 18) | (df_nat_hist['Hour'] < 6)]
            
            hist_stats = df_nat_hist.groupby(['Year', 'nightmarket_city', 'nightmarket_name']).agg(
                pdi_mean=('pdi_score', 'mean'),
                acc_count=('accident_id', 'count'),
                death_sum=('death_count', 'sum'),
                injury_sum=('injury_count', 'sum')
            ).reset_index()
            
            hist_stats['nat_rank'] = hist_stats.groupby('Year')['pdi_mean'].rank(ascending=False, method='min')
            hist_stats['city_rank'] = hist_stats.groupby(['Year', 'nightmarket_city'])['pdi_mean'].rank(ascending=False, method='min')
            
            target_hist = hist_stats[hist_stats['nightmarket_name'] == sel_market].copy()
            target_hist = target_hist.sort_values('Year', ascending=False).reset_index(drop=True)
            
            if not target_hist.empty:
                target_hist['平均 PDI'] = target_hist['pdi_mean'].apply(lambda x: f"{x:.1f}")
                target_hist['總事故件數'] = target_hist['acc_count'].apply(lambda x: f"{x:,.0f}")
                target_hist['死亡人數'] = target_hist['death_sum'].astype(int)
                target_hist['受傷人數'] = target_hist['injury_sum'].astype(int)
                target_hist['全國排名'] = target_hist['nat_rank'].apply(lambda x: f"第 {int(x)} 名")
                target_hist['縣市排名'] = target_hist['city_rank'].apply(lambda x: f"第 {int(x)} 名")
                
                final_table = target_hist[['Year', '平均 PDI', '總事故件數', '死亡人數', '受傷人數', '全國排名', '縣市排名']].rename(columns={'Year': '年份'})
                final_table['年份'] = final_table['年份'].astype(str)
                
                st.dataframe(final_table, use_container_width=True, hide_index=True)
            else:
                st.info("該夜市尚無歷年資料 (500m 核心區內無紀錄)。")
        else:
            st.info("無法獲取全台資料以計算歷年排名。")

# GROQ AI 生成單一夜市分析內容
# 串接 Groq API，將複雜的數據(死傷數、天氣比例、尖峰時刻、主要肇因等)包裝進 Prompt
@st.cache_data(ttl=3600, show_spinner=False)
def get_ai_analysis(market_name, total, pdi, dead, hurt, top_cause, peak_hour, rain, dark, wet, risky_loc):
    try:
        client = Groq(api_key=os.getenv("GROQ_API_KEY"))
        prompt = f"""
        你是一個交通專家。請分析「{market_name}」數據（總字數限制150字內）。
        數據：總事故{total}件、PDI指數{pdi:.1f}、死亡{dead}、受傷{hurt}。榜首肇因：{top_cause}。尖峰：{peak_hour}點。
        環境：雨天{rain:.1f}%、昏暗{dark:.1f}%、路濕{wet:.1f}%。
        最危險熱點：{risky_loc}
        
        請回答五大重點，且每個重點之間必須使用 Markdown 的雙換行 (`\n\n`) 來隔開：
        1. 肇因與預防。
        2. 綜合環境風險。
        3. 路段特徵推測。
        4. 熱點改善對策（附帶 Google Maps 網址連結）。
        5. 安全總結標語。
        格式：請用 Markdown 條列式，語氣專業，每次生成的內容，請固定都用 - 並且字體大小要相同。
        """
        res = client.chat.completions.create(messages=[{"role": "user", "content": prompt}], model="llama-3.3-70b-versatile")
        content = res.choices[0].message.content
        
        # 將連續單一換行取代為雙換行，確保 Streamlit 渲染出正確段落
        content = re.sub(r'(?<!\n)\n(?!\n)', '\n\n', content)
        return content
    except Exception as e: return f"⚠️ AI 暫時無法使用：{str(e)}"

if __name__ == "__main__":
    main()