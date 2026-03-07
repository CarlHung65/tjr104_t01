import streamlit as st
import pandas as pd
from datetime import timedelta
from data_loader import load_accidents

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

@st.cache_data
def get_date_metadata():
    # 取得所有事故日期（只查一次）
    latest_all = load_accidents(columns="accident_datetime")
    latest_all["accident_datetime"] = pd.to_datetime(latest_all["accident_datetime"])
    latest_date = latest_all["accident_datetime"].max()

    # 取得所有年份
    years = load_accidents(columns="DISTINCT YEAR(accident_datetime) AS year")["year"].tolist()

    # 每個年份有哪些月份
    months_by_year = {}
    for y in years:
        rows = load_accidents(
            columns="DISTINCT MONTH(accident_datetime) AS month",
            start_date=f"{y}-01-01",
            end_date=f"{y}-12-31"
        )
        months_by_year[y] = sorted(rows["month"].dropna().astype(int).tolist())


    return latest_date, years, months_by_year

def sidebar_filters(load_city_list_func, load_market_by_city_func):
    latest_date, years, months_by_year = get_date_metadata()

    # 1. 縣市
    city_list = load_city_list_func()
    city_list = [c for c in city_list if c not in (None, "", "null", "NULL")]
    selected_city = st.sidebar.selectbox("選擇縣市", city_list)

    # 2. 夜市
    market_list = load_market_by_city_func(selected_city)
    selected_market = st.sidebar.selectbox("選擇夜市", market_list)

    # 3. 日期模式
    date_filter_type = st.sidebar.selectbox(
        "篩選方式",
        ["月份", "年份（選月份）", "日期區間（最新7天）", "單一日期"]
    )

    date_info = {}

    if date_filter_type == "月份":
        # ⭐ 自動抓 months_by_year 裡的最新年份
        latest_year = max(months_by_year.keys())

        # ⭐ 取得該年份的月份列表
        months = months_by_year[latest_year]

        # ⭐ 顯示成 "1 (2026)" 這種格式
        month_options = [f"{m} ({latest_year})" for m in months]

        # ⭐ selectbox 顯示漂亮格式
        selected_month_label = st.sidebar.selectbox("月份", month_options)

        # ⭐ 再把真正的月份數字取回來
        month = int(selected_month_label.split(" ")[0])

        date_info["year"] = latest_year
        date_info["month"] = month

    elif date_filter_type == "年份（選月份）":
        year = st.sidebar.selectbox("年份", years)
        month = st.sidebar.selectbox("月份", months_by_year[year])
        date_info["year"] = year
        date_info["month"] = month

    elif date_filter_type == "日期區間（最新7天）":
        date_info["start_date"] = latest_date - timedelta(days=6)
        date_info["end_date"] = latest_date

    elif date_filter_type == "單一日期":
        date_info["date"] = st.sidebar.date_input("選擇日期", value=latest_date)

    return selected_city, selected_market, date_filter_type, date_info
