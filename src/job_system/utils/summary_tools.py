import streamlit as st
import pandas as pd
import utils.market_tools as mt

    # -------------------------
    # 單一夜市摘要器：summarize_single_market()
    # -------------------------

def summarize_ch1_single_market(
selected_market,
date_filtered_df,
day_df,
night_df,
pdi_df,
nm_acc
):
        # --- 基本事故統計 ---
        total = len(date_filtered_df)
        day_count = len(day_df)
        night_count = len(night_df)

        death = (date_filtered_df["death_count"] > 0).sum()
        injury = (date_filtered_df["injury_count"] > 0).sum()
        no_harm = total - death - injury

        # --- 最常發生時段 ---
        if total > 0:
            date_filtered_df["acc_time"] = pd.to_datetime(date_filtered_df["accident_datetime"])
            peak_hour = date_filtered_df["acc_time"].dt.hour.mode()[0]
            peak_text = f"{peak_hour} 點附近"
        else:
            peak_text = "無事故資料"

        # --- PDI 資料 ---
        row = pdi_df[pdi_df["夜市名稱"] == selected_market].iloc[0]
        pdi_value = row["PDI"]
        danger = row["危險等級"]

        # --- PDI 熱點摘要 ---
        if not nm_acc.empty:
            max_weight = nm_acc["pdi_weight"].max()
            total_points = len(nm_acc)
        else:
            max_weight = 0
            total_points = 0

        return f"""
    【{selected_market} — 夜市事故摘要】
    事故總數：{total} 件
    白天事故：{day_count} 件
    夜間事故：{night_count} 件

    死亡事故：{death} 件
    受傷事故：{injury} 件
    無死傷事故：{no_harm} 件

    事故最常發生時段：{peak_text}

    【{selected_market} — PDI（危險指數）】
    PDI：{pdi_value}
    危險等級：{danger}

    【{selected_market} — PDI 熱點摘要】
    事故點數：{total_points}
    最高 PDI 權重：{max_weight}
    """
 
   # -------------------------
    # 全台夜市 Summary 系統
    # -------------------------
def summarize_ch1_all_markets(
rating_df,
pdi_top4,
accident_top4,
pdi_df
):
        # --- 評分最高 ---
        top_rating = rating_df.iloc[0]
        top_rating_text = (
            f"{top_rating['夜市名稱']}（Google 評分 {top_rating['Google 評分']} 分，"
            f"{top_rating['危險等級']}）"
        )

        # --- 最危險夜市（PDI 最高） ---
        most_danger = pdi_df.iloc[0]
        most_danger_text = (
            f"{most_danger['夜市名稱']}（PDI {most_danger['PDI']}，"
            f"{most_danger['危險等級']}）"
        )

        # --- 最安全夜市（PDI 最低） ---
        safest_df = pdi_df[pdi_df["PDI"] == pdi_df["PDI"].min()]
        safest_list = ", ".join(safest_df["夜市名稱"].tolist())

        # --- PDI Top 4 ---
        pdi_lines = []
        for i, row in pdi_top4.iterrows():
            pdi_lines.append(
                f"{i+1}. {row['nightmarket_name']}（PDI {row['pdi']}，{mt.danger_level(row['pdi'])}）"
            )
        pdi_text = "\n".join(pdi_lines)

        # --- 事故數 Top 4 ---
        acc_lines = []
        for i, row in accident_top4.iterrows():
            acc_lines.append(
                f"{i+1}. {row['nightmarket_name']}（{row['accident_count']} 件）"
            )
        acc_text = "\n".join(acc_lines)

        return f"""
    【全台夜市 — 評分 vs 危險程度】
    評分最高：{top_rating_text}

    【全台夜市 — 最危險夜市】
    {most_danger_text}

    【全台夜市 — 最安全夜市】
    {safest_list}

    【全台夜市 — PDI Top 4】
    {pdi_text}

    【全台夜市 — 事故數 Top 4】
    {acc_text}
    """
def summarize_ch2(selected_market, templates):
    """
    第二章（事故分析）摘要，提供給 build_context 使用。
    """

    available_questions = "\n".join([
        "- 白天 vs 夜間事故差異",
        "- 24 小時事故高峰時段",
        "- 夜市營業時段的天氣風險",
        "- 雨天 vs 非雨天事故比較",
        "- 雨天事故熱點在哪裡",
        "- 夜市最常見的事故原因",
        "- 夜市最常見的違規行為",
        "- 夜市事故的死傷情況",
    ])

    return f"""
===== 夜市事故分析（第二章） =====
本章節整理「{selected_market}」夜市周邊的事故分布、時段變化、事故原因、天氣影響與風險指標，並加入事故資料的洞察摘要，協助理解夜市周邊的交通安全情形。

【事故群聚圖（白天／夜間）】
事故依照發生時段分為白天（06:00–17:59）與夜間（18:00–05:59），並以群聚圖呈現。每筆事故包含事故時間、死亡人數與受傷人數，使用者可切換圖層觀察兩個時段的差異。

【24 小時事故熱力動畫】
使用 HeatMapWithTime 呈現夜市 bounding box 內跨年度的事故時段變化。事故依照發生的小時（0–23 時）分類，熱力圖顯示不同時段的事故密度，可觀察事故是否在特定時段集中。

【事故原因分析（玫瑰圖／長條圖）】
分析夜市範圍內事故的人為因素（cause_analysis_minor_primary），以玫瑰圖或水平長條圖呈現各事故原因的比例與數量，協助了解事故的主要成因。

【雨天 vs 非雨天事故比較（營業時段）】
比較夜市營業時段（17–23 時）中雨天與非雨天的事故差異。提供事故量與事故嚴重度（死亡×3 + 受傷）兩種比較方式，用於觀察天氣是否影響事故發生頻率或嚴重程度。

【雨天事故熱點】
呈現夜市 bounding box 內的雨天事故熱力圖，顯示雨天事故在空間上的集中區域，並標示夜市位置與範圍框線，協助觀察雨天事故是否集中在特定路段或夜市出入口。

【營業時段天氣風險雷達圖】
分析夜市營業時段（17–23 時）的三項天氣相關風險：雨天、光線不佳與濕滑路面。雷達圖顯示這三項風險在事故中出現的比例，協助理解夜市營業時段的天氣風險輪廓。

===== 事故洞察（自動生成） =====
- {templates[0]}
- {templates[1]}
- {templates[2]}
- {templates[3]}
- {templates[4]}

===== 你可以詢問的項目 =====
{available_questions}

===== 目前選取的夜市 =====
{selected_market}
"""
def summarize_ch3(selected_market, grid_df, exit_name, route_instructions, stats, templates):
    """
    第三章（區段危險等級 + 推薦入口 + 導航路線 + 行人安全提醒）摘要
    """

    # 區塊統計
    red_blocks = len(grid_df[grid_df["color"] == "red"])
    yellow_blocks = len(grid_df[grid_df["color"] == "yellow"])
    green_blocks = len(grid_df[grid_df["color"] == "green"])

    # 區塊最高風險
    top_block = grid_df.sort_values("score", ascending=False).iloc[0]
    top_score = top_block["score"]
    top_accidents = top_block["accident_count"]
    top_row = top_block["grid_row"]
    top_col = top_block["grid_col"]

    # 導航步驟
    if route_instructions:
        route_text = "\n".join([f"- {step}" for step in route_instructions])
    else:
        route_text = "（無法取得導航路線，可能是 OSRM 服務暫時無法連線）"

    # 行人安全提醒
    peak_period = stats["peak_period"]
    rain_increase = stats["rain_increase"]
    danger_zone = stats["danger_zone"]
    best_entry = stats["best_entry"]

    available_questions = "\n".join([
        "- 夜市周邊哪些區塊最危險？",
        "- 夜市周邊哪些區塊最安全？",
        "- 夜市周邊的事故熱點在哪裡？",
        "- 夜市周邊的 3×3 區塊風險分布如何？",
        "- 夜市周邊是否有高風險入口？",
        "- 夜市推薦入口在哪裡？",
        "- 推薦入口怎麼走？",
        "- 導航路線有哪些步驟？",
        "- 夜市最危險的時段是什麼？",
        "- 雨天事故會增加多少？",
        "- 哪個方向的事故最多？",
    ])

    return f"""
===== 夜市區段危險等級分析（第三章） =====
本章節分析「{selected_market}」夜市周邊的 3×3 區段危險等級、事故點分布、推薦入口、導航路線與行人安全提醒，協助找出夜市周邊的高風險區域與最安全的進入路線。

【3×3 區段風險分布】
夜市周邊被切成 3×3 共 9 個區塊，每個區塊依照事故風險分數（死亡×3 + 受傷）標示顏色：
- 紅色：高風險（score ≥ 6）
- 黃色：中風險（3 ≤ score < 6）
- 綠色：低風險（score < 3）

【區段統計】
- 高風險（紅色）區塊：{red_blocks} 個
- 中風險（黃色）區塊：{yellow_blocks} 個
- 低風險（綠色）區塊：{green_blocks} 個

【最高風險區塊】
- 區塊位置：第 {top_row+1} 行、第 {top_col+1} 列
- 風險分數：{top_score}
- 事故數量：{top_accidents}

【事故點分布（CircleMarker）】
每個事故點依照死傷程度標示顏色：
- 紅色：死亡事故
- 藍色：受傷事故
- 淺藍：無死傷事故

【推薦入口（最遠離事故點）】
- 推薦入口：{exit_name}

【導航路線（OSRM 步行路線）】
以下為從推薦入口步行至夜市中心的建議路線：

{route_text}

【行人安全提醒（四宮格）】
- 最危險時段：{peak_period}
- 雨天事故提升：{rain_increase}%
- 事故密度最高區域：{danger_zone}
- 建議從 {best_entry} 進入夜市

===== 區段洞察（自動生成） =====
- {templates[0]}
- {templates[1]}
- {templates[2]}
- {templates[3]}
- {templates[4]}

===== 你可以詢問的項目 =====
{available_questions}

===== 目前選取的夜市 =====
{selected_market}
"""

def summarize_ch4(
    selected_city,
    date_filtered_df,
    city_rank,
    pdi_df,
    time_risk,
    weather_risk,
    light_risk,
    templates
):
    # -------------------------
    # 全台事故統計
    # -------------------------
    total = len(date_filtered_df)
    deaths = (date_filtered_df["death_count"] > 0).sum()
    injuries = (date_filtered_df["injury_count"] > 0).sum()

    if "county" in date_filtered_df.columns and total > 0:
        top_county = date_filtered_df["county"].mode()[0]
        top_county_count = date_filtered_df["county"].value_counts().max()
        county_text = f"{top_county}（{top_county_count} 件）"
    else:
        county_text = "無資料"

    # -------------------------
    # 城市危險程度（city_rank）
    # -------------------------
    if len(city_rank) > 0:
        most_danger = city_rank.iloc[0]
        most_danger_text = (
            f"{most_danger['城市']}（平均PDI {most_danger['平均PDI']}，"
            f"{most_danger['危險等級']}）"
        )

        safest_df = city_rank[city_rank["平均PDI"] == city_rank["平均PDI"].min()]
        safest_list = ", ".join(safest_df["城市"].tolist())
    else:
        most_danger_text = "無資料"
        safest_list = "無資料"

    rank_lines = []
    for i, row in city_rank.head(5).iterrows():
        rank_lines.append(
            f"{i+1}. {row['城市']}（平均PDI {row['平均PDI']}，{row['危險等級']}）"
        )
    rank_text = "\n".join(rank_lines) if rank_lines else "無資料"

    # -------------------------
    # 縣市內夜市 PDI 排名（pdi_df）
    # -------------------------
    nm_lines = []
    for i, row in pdi_df.iterrows():
        nm_lines.append(
            f"{i+1}. {row['夜市名稱']}（PDI {row['PDI']}，{row['危險等級']}，事故 {row['事故數']} 件）"
        )
    nm_text = "\n".join(nm_lines) if nm_lines else "無資料"

    # -------------------------
    # 高風險時段（time_risk）
    # -------------------------
    if time_risk is not None and len(time_risk) > 0:
        peak_row = time_risk.sort_values("事故數", ascending=False).iloc[0]
        peak_hour = peak_row["accident_hour"]
        peak_count = peak_row["事故數"]
        time_text = f"{peak_hour} 時（{peak_count} 件）"
    else:
        time_text = "無資料"

    # -------------------------
    # 天氣風險（weather_risk）
    # -------------------------
    if weather_risk is not None and len(weather_risk) > 0:
        weather_lines = []
        for _, row in weather_risk.iterrows():
            weather_lines.append(
                f"{row['天氣狀況']}：{row['事故數']} 件（{row['比例 (%)']}%）"
            )
        weather_text = "\n".join(weather_lines)
    else:
        weather_text = "無資料"

    # -------------------------
    # 道路環境風險（light_risk）
    # -------------------------
    if light_risk is not None and len(light_risk) > 0:
        light_lines = []
        for _, row in light_risk.iterrows():
            light_lines.append(
                f"{row['光線狀況']}：{row['事故數']} 件（{row['比例 (%)']}%）"
            )
        light_text = "\n".join(light_lines)
    else:
        light_text = "無資料"

    # -------------------------
    # AI 洞察（templates）
    # -------------------------
    insight_block = "\n".join([f"- {t}" for t in templates])

    # -------------------------
    # 組合 CH4 摘要
    # -------------------------
    return f"""
【第四章 — 全台事故熱力圖與縣市風險分析】

【全台事故統計】
事故總數：{total} 件
死亡事故：{deaths} 件
受傷事故：{injuries} 件

【事故最密集縣市】
{county_text}

【城市危險程度（PDI）】
最危險城市：{most_danger_text}
最安全城市：{safest_list}

【城市危險程度排名（Top 5）】
{rank_text}

【{selected_city} — 夜市事故密度與 PDI 排名】
{nm_text}

【{selected_city} — 高風險時段】
最危險時段：{time_text}

【{selected_city} — 天氣風險】
{weather_text}

【{selected_city} — 道路環境風險】
{light_text}

【AI 洞察（自動生成）】
{insight_block}
"""