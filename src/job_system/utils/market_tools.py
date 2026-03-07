import streamlit as st
import pandas as pd
import random
import re
import datetime

# -------------------------
# 工具：洞察生成器 V1
# -------------------------
def generate_insight_V1(acc_df, market_df, selected_market):
    # 找到夜市座標
    nm_row = market_df[market_df["nightmarket_name"] == selected_market]
    if nm_row.empty:
        return "找不到夜市資料。"
    
    nm = nm_row.iloc[0]
    nm_lat = float(nm["nightmarket_latitude"])
    nm_lon = float(nm["nightmarket_longitude"])

    # 計算事故距離
    acc_df["distance"] = ((acc_df["latitude"].astype(float) - nm_lat)**2 +
                          (acc_df["longitude"].astype(float) - nm_lon)**2)**0.5

    # 取 500 公尺內事故
    nearby = acc_df[acc_df["distance"] < 0.005]

    if nearby.empty:
        return f"{selected_market} 周邊目前沒有事故資料。"

    # 數據
    total = len(nearby)
    deaths = nearby["death_count"].sum()
    injuries = nearby["injury_count"].sum()

    nearby["hour"] = pd.to_datetime(nearby["accident_datetime"]).dt.hour
    peak_hour = nearby["hour"].mode()[0]

    # 時段分類
    if 5 <= peak_hour < 12:
        time_label = "早上"
    elif 12 <= peak_hour < 17:
        time_label = "下午"
    elif 17 <= peak_hour < 22:
        time_label = "晚上"
    else:
        time_label = "深夜"

    # -------------------------
    # A. 事故量句子（多個模板）
    # -------------------------
    accident_templates = [
        f"<u>{selected_market}</u>  周邊共有 {total} 件事故，造成 {deaths} 人死亡、{injuries} 人受傷。",
        f"<u>{selected_market}</u>  周邊發生 {total} 件事故，死傷共 {deaths + injuries} 人。",
        f"<u>{selected_market}</u>  附近近期累計 {total} 件事故，造成 {deaths} 死 {injuries} 傷。",
        f"<u>{selected_market}</u>  周邊事故量為 {total} 件，死傷情況為 {deaths} 死亡、{injuries} 受傷。",
    ]

    # -------------------------
    # B. 時段句子（多個模板）
    # -------------------------
    time_templates = [
        f"<u>{selected_market}</u>  事故最常發生在 {time_label}（約 {peak_hour} 時）。",
        f"<u>{selected_market}</u>  事故高峰落在 {time_label}（約 {peak_hour} 時）。",
        f"<u>{selected_market}</u>  最容易發生事故的時段是 {time_label}（約 {peak_hour} 時）。",
        f"<u>{selected_market}</u>  事故多集中在 {time_label}（約 {peak_hour} 時）。",
    ]

    # -------------------------
    # ⭐ 每次只顯示其中一種（隨機 A 或 B）
    # -------------------------
    chosen_group = random.choice([accident_templates, time_templates])
    return random.choice(chosen_group)

# -------------------------
# 工具：洞察生成器 V2
# -------------------------

def generate_insight_V2(acc_df, market_df, selected_market,
                        human_df=None, road_df=None):

    # -------------------------
    # 找夜市座標
    # -------------------------
    nm_row = market_df[market_df["nightmarket_name"] == selected_market]
    if nm_row.empty:
        return "找不到夜市資料。"

    nm = nm_row.iloc[0]
    nm_lat = float(nm["nightmarket_latitude"])
    nm_lon = float(nm["nightmarket_longitude"])

    # -------------------------
    # 計算事故距離
    # -------------------------
    acc_df = acc_df.copy()
    acc_df["distance"] = ((acc_df["latitude"].astype(float) - nm_lat)**2 +
                          (acc_df["longitude"].astype(float) - nm_lon)**2)**0.5

    # -------------------------
    # 取 500 公尺內事故（0.005 度 ≈ 500m）
    # -------------------------
    nearby = acc_df[acc_df["distance"] < 0.005]

    if nearby.empty:
        return f"{selected_market} 周邊目前沒有事故資料。"

    df = nearby.copy()

    # -------------------------
    # 合併人為因素、道路因素
    # -------------------------
    if human_df is not None:
        df = df.merge(human_df, on="accident_id", how="left")

    if road_df is not None:
        df = df.merge(road_df, on="accident_id", how="left")

    # -------------------------
    # 基本數據
    # -------------------------
    total = len(df)
    deaths = df["death_count"].sum()
    injuries = df["injury_count"].sum()
    severity = deaths * 5 + injuries * 2

    # -------------------------
    # 時段
    # -------------------------
    df["hour"] = pd.to_datetime(df["accident_datetime"], errors="coerce").dt.hour
    if df["hour"].notna().any():
        peak_hour = int(df["hour"].mode()[0])
    else:
        peak_hour = None

    if peak_hour is None:
        time_label = "不明時段"
    elif 5 <= peak_hour < 12:
        time_label = "早上"
    elif 12 <= peak_hour < 17:
        time_label = "下午"
    elif 17 <= peak_hour < 22:
        time_label = "晚上"
    else:
        time_label = "深夜"

    # -------------------------
    # 事故原因
    # -------------------------
    if "cause_analysis_minor_primary" in df and df["cause_analysis_minor_primary"].notna().any():
        top_cause = df["cause_analysis_minor_primary"].mode()[0]
    else:
        top_cause = "無事故原因資料"

    # -------------------------
    # 違規行為
    # -------------------------
    if "party_action_minor" in df and df["party_action_minor"].notna().any():
        top_violation = df["party_action_minor"].mode()[0]
    else:
        top_violation = "無違規資料"

    # -------------------------
    # 洞察模板
    # -------------------------
    templates = [
        f"<u>{selected_market}</u> 周邊 500 公尺內共發生 {total} 件事故，死傷 {deaths + injuries} 人，事故高峰多出現在 {time_label}（約 {peak_hour} 時）。",
        f"<u>{selected_market}</u> 近期事故量為 {total} 件，主要事故原因為「{top_cause}」，最常見違規行為為「{top_violation}」。",
        f"<u>{selected_market}</u> 周邊事故造成 {deaths} 死亡、{injuries} 受傷，事故多集中在 {time_label}（{peak_hour} 時）。",
        f"<u>{selected_market}</u> 夜市附近事故以「{top_cause}」最常見，並在 {time_label} 時段達到高峰。",
        f"<u>{selected_market}</u> 事故嚴重度評估為 {severity} 分，主要違規行為為「{top_violation}」。"
    ]

    return random.choice(templates)


# -------------------------
# 工具：洞察生成器 V3
# -------------------------

def generate_insight_V3(selected_market, grid_df, best_exit):

    # -------------------------
    # 工具：把格子座標轉成方向（東北、西南、東南、西北）
    # -------------------------
    def cell_to_direction(row, col, grid_size=3):
        center = grid_size // 2  # 3x3 → center = 1

        # 四角方向
        if row < center and col < center:
            return "西北"
        if row < center and col > center:
            return "東北"
        if row > center and col < center:
            return "西南"
        if row > center and col > center:
            return "東南"

        # 正方向
        if row == center and col < center:
            return "正西"
        if row == center and col > center:
            return "正東"
        if col == center and row < center:
            return "正北"
        if col == center and row > center:
            return "正南"

        return "中心"

    # -------------------------
    # 出口名稱（避免顯示座標）
    # -------------------------
    best_exit_name = best_exit[0] if isinstance(best_exit, (list, tuple)) else best_exit

    # -------------------------
    # 無資料
    # -------------------------
    if grid_df.empty:
        return f"{selected_market} 周邊目前沒有事故資料。"

    # -------------------------
    # 區塊分類
    # -------------------------
    high_risk_cells = grid_df[grid_df["color"] == "red"]
    mid_risk_cells = grid_df[grid_df["color"] == "yellow"]
    low_risk_cells = grid_df[grid_df["color"] == "green"]

    # -------------------------
    # 最危險區塊
    # -------------------------
    if not high_risk_cells.empty:
        worst_cell = high_risk_cells.sort_values("score", ascending=False).iloc[0]
        worst_score = int(worst_cell["score"])
        worst_count = int(worst_cell["accident_count"])
        worst_dir = cell_to_direction(worst_cell["grid_row"], worst_cell["grid_col"])
    else:
        worst_dir = "無"
        worst_score = 0
        worst_count = 0

    # -------------------------
    # 最安全區塊
    # -------------------------
    if not low_risk_cells.empty:
        safest_cell = low_risk_cells.sort_values("score", ascending=True).iloc[0]
        safest_dir = cell_to_direction(safest_cell["grid_row"], safest_cell["grid_col"])
    else:
        safest_dir = "無"

    # -------------------------
    # 區段風險比例
    # -------------------------
    total_cells = len(grid_df)
    high_ratio = round(len(high_risk_cells) / total_cells * 100, 1)
    mid_ratio = round(len(mid_risk_cells) / total_cells * 100, 1)
    low_ratio = round(len(low_risk_cells) / total_cells * 100, 1)

    # -------------------------
    # 洞察模板（第三章：區段危險等級）
    # -------------------------
    templates = [
        # 1. 最危險區塊
        f"{selected_market}最危險的區段位於 {worst_dir}側，累積風險分數 {worst_score}，共發生 {worst_count} 件事故。",

        # 2. 最安全區塊
        f"{selected_market}最安全的區段位於 {safest_dir}側，適合作為行人通行或避開事故熱點的路線。",

        # 3. 推薦入口
        f"建議從 {best_exit_name} 進入{selected_market}，該入口距離事故點最遠，安全性最高。",

        # 4. 全面型洞察
        f"整體來看，{selected_market}最高風險集中在 {worst_dir}側，最安全區段則位於 {safest_dir}側，從 {best_exit_name} 進入能有效降低風險。"
    ]

    return random.choice(templates)

# -------------------------
# 工具：洞察生成器 V4
# -------------------------

def generate_insight_V4(city_rank):
    import random

    if city_rank.empty:
        return "目前沒有足夠的資料產生城市排行榜洞察。"

    # 依平均PDI排序
    sorted_rank = city_rank.sort_values("平均PDI", ascending=False)

    # 最危險城市
    top_city = sorted_rank.iloc[0]
    top_name = top_city["城市"]
    top_pdi = top_city["平均PDI"]
    top_level = top_city["危險等級"]
    top_acc = top_city["事故總數"]
    top_nm = top_city["夜市數量"]

    # 最安全城市
    safe_city = sorted_rank.iloc[-1]
    safe_name = safe_city["城市"]
    safe_pdi = safe_city["平均PDI"]
    safe_level = safe_city["危險等級"]
    safe_acc = safe_city["事故總數"]
    safe_nm = safe_city["夜市數量"]

    # 全體平均
    avg_pdi = round(city_rank["平均PDI"].mean(), 1)

    # -------------------------
    # 洞察模板（直敘句版本）
    # -------------------------
    templates = [

        # 1. 危險城市洞察
        f"{top_name} 的夜市平均 PDI 達到 {top_pdi}（{top_level}），是目前最需要改善行人安全的城市之一，共有 {top_nm} 個夜市、累積 {top_acc} 件事故。",

        # 2. 安全城市洞察
        f"{safe_name} 的夜市平均 PDI 僅 {safe_pdi}（{safe_level}），在所有城市中相對安全，事故量 {safe_acc} 件、夜市數量 {safe_nm} 個。",

        # 3. 危險 vs 安全對比
        f"{top_name} 與 {safe_name} 的夜市安全差距明顯，前者平均 PDI 高達 {top_pdi}，後者僅 {safe_pdi}，顯示不同城市之間的夜市風險差異巨大。",

        # 4. 全體概況
        f"全台夜市城市的平均 PDI 約為 {avg_pdi}，其中 {top_name} 風險最高，而 {safe_name} 相對最安全。",

        # 5. 綜合洞察
        f"整體來看，{top_name} 的夜市風險集中且事故量高，而 {safe_name} 的夜市相對安全，城市之間的夜市安全程度呈現明顯落差。"
    ]

    return random.choice(templates)

    # -------------------------
    # tooltip（滑過顯示）
    # -------------------------
def build_tooltip(df):
    df = df.copy()
    df["accident_datetime"] = pd.to_datetime(df["accident_datetime"], errors="coerce")

    df["tooltip_text"] = df.apply(
        lambda row: (
            f"事故時間：{row['accident_datetime'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['accident_datetime']) else '未知'}\n"
            f"死亡：{int(row['death_count']) if pd.notna(row['death_count']) else 0} 人\n"
            f"受傷：{int(row['injury_count']) if pd.notna(row['injury_count']) else 0} 人"
        ),
        axis=1
    )

    return df

# ---------------------------------------------------------
# 工具：解析夜市營業時段
# ---------------------------------------------------------
def parse_opening_hours(opening_str):
    segments = opening_str.split("/")
    result = {}

    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue

        weekday_match = re.search(r'weekday"\s*:\s*(\d+)', seg)
        open_match = re.search(r'open"\s*:\s*"(\d{2}:\d{2})"', seg)
        close_match = re.search(r'close"\s*:\s*"(\d{2}:\d{2})"', seg)

        if weekday_match and open_match and close_match:
            weekday = int(weekday_match.group(1))
            result[weekday] = (open_match.group(1), close_match.group(1))

    return result


# ---------------------------------------------------------
# 工具：判斷事故是否在夜市營業時段
# ---------------------------------------------------------
def is_in_opening(acc_time, opening_hours):
    weekday = acc_time.weekday()

    if weekday not in opening_hours:
        return False

    start_str, end_str = opening_hours[weekday]
    start = datetime.datetime.strptime(start_str, "%H:%M").time()
    end = datetime.datetime.strptime(end_str, "%H:%M").time()

    if start <= end:
        return start <= acc_time.time() <= end
    else:
        return acc_time.time() >= start or acc_time.time() <= end


# ---------------------------------------------------------
# 工具：取得夜市 bounding box
# ---------------------------------------------------------
def get_bbox(nm):
    return {
        "ne_lat": float(nm["nightmarket_northeast_latitude"]),
        "ne_lon": float(nm["nightmarket_northeast_longitude"]),
        "sw_lat": float(nm["nightmarket_southwest_latitude"]),
        "sw_lon": float(nm["nightmarket_southwest_longitude"]),
    }

# ---------------------------------------------------------
# 工具：判斷事故是否在夜市 bounding box 內
# ---------------------------------------------------------
def accident_in_bbox(lat, lon, bbox):
    return (
        bbox["sw_lat"] <= lat <= bbox["ne_lat"] and
        bbox["sw_lon"] <= lon <= bbox["ne_lon"]
    )

# ---------------------------------------------------------
# 完整 PDI 計算
# ---------------------------------------------------------
def calculate_pdi(acc_df, nm_df):
    WEIGHT_DEATH = 5
    WEIGHT_INJURY = 2
    WEIGHT_OPEN = 3
    WEIGHT_CLOSE = 1

    results = []

    for _, nm in nm_df.iterrows():
        bbox = get_bbox(nm)
        opening_hours = parse_opening_hours(nm["nightmarket_opening_hours"])

        acc_in_nm = acc_df[
            acc_df.apply(
                lambda row: accident_in_bbox(row["latitude"], row["longitude"], bbox),
                axis=1
            )
        ]

        total_pdi = 0

        for _, acc in acc_in_nm.iterrows():
            # 這裡改掉，不要用 strptime
            acc_time = acc["accident_datetime"]   # <-- 已經是 Timestamp

            severity = acc["death_count"] * WEIGHT_DEATH + acc["injury_count"] * WEIGHT_INJURY
            weight = WEIGHT_OPEN if is_in_opening(acc_time, opening_hours) else WEIGHT_CLOSE

            total_pdi += severity * weight

        results.append({
            "nightmarket_name": nm["nightmarket_name"],
            "accident_count": len(acc_in_nm),
            "pdi": total_pdi
        })

    return pd.DataFrame(results)

# ---------------------------------------------------------
# HTML 模板
# ---------------------------------------------------------
#HTML「完全無縮排、無空白行」

def html_template(): """
<a href="{url}" target="_blank" style="text-decoration:none;display:inline-block;">
<div class="pdi-card" style="width:clamp(180px,30vw,260px);padding:15px;border-radius:12px;background-color:#ffffff;box-shadow:0 4px 10px rgba(0,0,0,0.15);text-align:center;border:1px solid #eee;transition:transform 0.15s ease, box-shadow 0.15s ease;cursor:pointer;">
<div style="font-size:clamp(16px,2vw,22px);font-weight:bold;margin-bottom:8px;color:#333;">🥇 第 {rank} 名</div>
<div style="font-size:clamp(14px,2vw,20px);font-weight:bold;color:#333;margin-bottom:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{name}</div>
<div style="font-size:clamp(12px,1.8vw,18px);margin-bottom:4px;color:#333;">PDI：<b>{pdi}</b>（{level}）</div>
<div style="font-size:clamp(12px,1.5vw,16px);color:#555;">事故數：{count} 件</div>
</div>
</a>
"""

# -----------------------------------------------------
# 全域 function 區（ 顏色 ）
# -----------------------------------------------------

def danger_level(pdi):
    if pdi <= 10:
        return "🟢 安全"
    elif pdi <= 30:
        return "🟡 注意"
    elif pdi <= 60:
        return "🟠 危險"
    else:
        return "🔴 極危險"

def danger_color(pdi):
    if pdi <= 10:
        return "linear-gradient(135deg, #81c784, #43a047)"  # 綠（強烈）
    elif pdi <= 30:
        return "linear-gradient(135deg, #fff176, #fdd835)"  # 黃（強烈）
    elif pdi <= 60:
        return "linear-gradient(135deg, #ffcc80, #ff7043)"  # 橘（強烈）
    else:
        return "linear-gradient(135deg, #ff8a80, #e53935)"  # 紅（強烈）

def pdi_divider(level):
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
        background: {colors[level]};
        border-radius: 3px;
    ">
    """, unsafe_allow_html=True)

