import streamlit as st
import pandas as pd
import utils.summary_tools as s_tool

# -----------------------------------------------------
# AI 小幫手相關
# -----------------------------------------------------
def save_page_data(page_name, filters, table, summary):
    if "page_data" not in st.session_state:
        st.session_state["page_data"] = []

    st.session_state["page_data"].append({
        "page": page_name,
        "filters": filters,
        "table": table,
        "summary": summary
    })

# ---------------------------------------------------------
# ⭐ 收集前面章節資料（AI 可引用）
# ---------------------------------------------------------

def build_context(current_chapter, **kwargs):

    # ============================
    # 第一章
    # ============================
    if current_chapter == 1:

        summary_single = s_tool.summarize_ch1_single_market(
            kwargs["selected_market"],
            kwargs["date_filtered_df"],
            kwargs["day_df"],
            kwargs["night_df"],
            kwargs["pdi_df"],
            kwargs["nm_acc"]
        )

        summary_all = s_tool.summarize_ch1_all_markets(
            kwargs["rating_df"],
            kwargs["pdi_top4"],
            kwargs["accident_top4"],
            kwargs["pdi_df"]
        )

        # ⭐ 夜市危險等級
        pdi_list = "\n".join(
            f"{row['夜市名稱']}：事故 {row['事故數']} 件，PDI {row['PDI']}，危險等級 {row['危險等級']}"
            for _, row in kwargs["pdi_df"].iterrows()
        )

        # ⭐ Google 評分（你之前缺少這段）
        rating_list = "\n".join(
            f"{row['夜市名稱']}：Google 評分 {row['Google 評分']}"
            for _, row in kwargs["rating_df"].iterrows()
        )

        # ⭐ 可詢問項目（危險等級 + Google 評分）
        available_questions = "\n".join([
            f"- {row['夜市名稱']} 的危險等級"
            for _, row in kwargs["pdi_df"].iterrows()
        ] + [
            f"- {row['夜市名稱']} 的 Google 評分"
            for _, row in kwargs["rating_df"].iterrows()
        ])

        # ⭐ 最終 context（加入分隔線，LLM 才能解析）
        return f"""
===== 單一夜市資訊 =====
{summary_single}

===== 全台夜市資訊 =====
{summary_all}

===== 所有夜市危險等級列表 =====
{pdi_list}

===== 夜市 Google 評分 =====
{rating_list}

===== 你可以詢問的項目 =====
{available_questions}

===== 目前選取的夜市 =====
{kwargs["selected_market"]}
"""
    # ============================
    # 第二章
    # ============================
    if current_chapter == 2:

        selected_market = kwargs["selected_market"]
        templates = kwargs["templates"]   # 你在主程式產生的 5 句洞察

        # ⭐ 可詢問項目（依照第二章內容）
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

        # ⭐ 第二章 summary
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
    
    # ============================
    # 第三章
    # ============================
    if current_chapter == 3:
        return s_tool.summarize_ch3(
            kwargs["selected_market"],
            kwargs["grid_df"],
            kwargs["exit_name"],
            kwargs["route_instructions"],
            kwargs["stats"],
            kwargs["templates"]
        )
            
    # ============================
    # 第四章
    # ============================
    if current_chapter == 4:
        return s_tool.summarize_ch4(
            kwargs["selected_city"],
            kwargs["date_filtered_df"],
            kwargs["city_rank"],
            kwargs["pdi_df"],
            kwargs["time_risk"],
            kwargs["weather_risk"],
            kwargs["light_risk"],
            kwargs["templates"]
        )
