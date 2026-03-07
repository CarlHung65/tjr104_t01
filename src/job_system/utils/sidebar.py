import streamlit as st
from datetime import timedelta


def sidebar_background():
    sidebar_bg = """
    <style>
    /* 讓 sidebar 容器可疊加背景 */
    section[data-testid="stSidebar"] {
        position: relative;
        background-color: transparent !important;
    }

    /* ⭐ 背景圖層（透明度 50%） */
    section[data-testid="stSidebar"]::before {
        content: "";
        position: absolute;
        inset: 0;  /* 等同 top:0; left:0; width:100%; height:100%; */
        background-image: url("https://images.pexels.com/photos/810890/nature-night-sky-milky-way-810890.jpeg?auto=compress&cs=tinysrgb&w=800");
        background-size: cover;
        background-position: center;
        opacity: 0.35;  /* ⭐ 只有背景透明 */
        z-index: 0;    /* ⭐ 放在最底層 */
    }

    /* ⭐ sidebar 內容保持在最上層，不透明 */
    section[data-testid="stSidebar"] > div {
        position: relative;
        z-index: 1;
    }
    </style>
    """
    st.markdown(sidebar_bg, unsafe_allow_html=True)

def sidebar_filters(load_city_list_func, load_market_by_city_func, accidents_df):
    """
    工具層：整合夜市選擇 + 時間篩選（含資料日期範圍限制）
    回傳：
        selected_city：選到的縣市
        selected_market：選到的夜市
        filtered_df：依時間篩選後的事故資料
        filter_type：篩選方式
        date_info：包含年份、月份、日期區間、單一日期
    """

    # -----------------------------
    # 0. 取得資料的最早與最新日期
    # -----------------------------
    min_date = accidents_df["accident_datetime"].dt.date.min()
    max_date = accidents_df["accident_datetime"].dt.date.max()

    # -----------------------------
    # 1. 夜市選擇
    # -----------------------------
    city_list = load_city_list_func()
    city_list = [c for c in city_list if c is not None]

    default_city = "台北市" if "台北市" in city_list else city_list[0]

    selected_city = st.sidebar.selectbox(
        "選擇縣市",
        city_list,
        index=city_list.index(default_city)
    )

    market_list = load_market_by_city_func(selected_city)
    default_market = "饒河街觀光夜市" if "饒河街觀光夜市" in market_list else market_list[0]

    selected_market = st.sidebar.selectbox(
        "選擇夜市",
        market_list,
        index=market_list.index(default_market)
    )

    # -----------------------------
    # 2. 時間篩選
    # -----------------------------
    date_filter_type = st.sidebar.selectbox(
        "篩選方式",
        ["月份", "年份", "日期區間（7 天）", "自訂日期區間", "單一日期"]
    )

    # ⭐ 用來回傳給功能頁的日期資訊
    date_info = {
        "year": None,
        "month": None,
        "start_date": None,
        "end_date": None,
        "date": None
    }

    # 月份
    if date_filter_type == "月份":
        years = accidents_df["accident_datetime"].dt.year.unique()
        year = st.sidebar.selectbox("選擇年份", years)
        month = st.sidebar.selectbox("選擇月份", range(1, 13), format_func=lambda x: f"{x} 月")

        date_info["year"] = year
        date_info["month"] = month

        date_filtered_df = accidents_df[
            (accidents_df["accident_datetime"].dt.year == year) &
            (accidents_df["accident_datetime"].dt.month == month)
        ]

    # 年份
    elif date_filter_type == "年份":
        years = accidents_df["accident_datetime"].dt.year.unique()
        year = st.sidebar.selectbox("選擇年份", years)

        date_info["year"] = year

        date_filtered_df = accidents_df[
            accidents_df["accident_datetime"].dt.year == year
        ]

    # 固定 7 天
    elif date_filter_type == "日期區間（7 天）":
        start_date = st.sidebar.date_input(
            "選擇起始日",
            value=max_date - timedelta(days=7),
            min_value=min_date,
            max_value=max_date
        )
        end_date = min(start_date + timedelta(days=7), max_date)

        date_info["start_date"] = start_date
        date_info["end_date"] = end_date

        date_filtered_df = accidents_df[
            (accidents_df["accident_datetime"].dt.date >= start_date) &
            (accidents_df["accident_datetime"].dt.date <= end_date)
        ]

    # 自訂日期區間
    elif date_filter_type == "自訂日期區間":
        start_date = st.sidebar.date_input("開始日期", value=min_date, min_value=min_date, max_value=max_date)
        end_date = st.sidebar.date_input("結束日期", value=max_date, min_value=min_date, max_value=max_date)

        date_info["start_date"] = start_date
        date_info["end_date"] = end_date

        date_filtered_df = accidents_df[
            (accidents_df["accident_datetime"].dt.date >= start_date) &
            (accidents_df["accident_datetime"].dt.date <= end_date)
        ]

    # 單一日期
    elif date_filter_type == "單一日期":
        selected_date = st.sidebar.date_input("選擇日期", value=max_date, min_value=min_date, max_value=max_date)

        date_info["date"] = selected_date

        date_filtered_df = accidents_df[
            accidents_df["accident_datetime"].dt.date == selected_date
        ]

    # ⭐ 多回傳 date_info
    return selected_city, selected_market, date_filtered_df, date_filter_type, date_info





    