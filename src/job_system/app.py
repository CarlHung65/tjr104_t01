import streamlit as st
# -------------------------
# AI 小幫手
# -------------------------
st.set_page_config(page_title="Map Chat", page_icon="💬")
from home import render_home
from page.act2_overview import act2_render
from page.act3_avoid import act3_render
from page.act4_gov import act4_render
from page.act5_policy import act5_render
from page.act6_chat import act6_render

# -------------------------
# Sidebar 背景底圖
# -------------------------

sidebar_bg = """
<style>

/* ⭐ Sidebar 內容容器（你的版本） */
div[data-testid="stSidebarContent"] {
    position: relative !important;
    background-color: transparent !important;
}

/* ⭐ 背景圖層 */
div[data-testid="stSidebarContent"]::before {
    content: "";
    position: absolute;
    inset: 0;
    background-image: url("https://images.pexels.com/photos/810890/nature-night-sky-milky-way-810890.jpeg?auto=compress&cs=tinysrgb&w=800");
    background-size: cover;
    background-position: center;
    opacity: 0.6;
    z-index: 0;
}

/* ⭐ Sidebar 內容保持在最上層 */
div[data-testid="stSidebarContent"] > div {
    position: relative;
    z-index: 1;
}

</style>
"""
st.markdown(sidebar_bg, unsafe_allow_html=True)

# -------------------------
# 主頁寬度
# -------------------------

st.markdown("""
<style>
.block-container {
    max-width: 1000px;
    margin-left: auto;
    margin-right: auto;
}
</style>
""", unsafe_allow_html=True)


# -------------------------
# Sidebar 寬度
# -------------------------

st.markdown("""
<style>
section[data-testid="stSidebar"] {
    width: 450px !important;
}
</style>
""", unsafe_allow_html=True)


# -------------------------
# Sidebar 字體
# -------------------------

st.markdown("""
<style>

div[data-testid="stSidebarContent"] * {
    font-size: 25px !important;
}

/* radio 標題 */
div[data-testid="stSidebarContent"] .stRadio label {
    font-size: 25px !important;
    font-weight: 600;
}

/* selectbox 標題 */
div[data-testid="stSidebarContent"] .stSelectbox label {
    font-size: 25px !important;
    font-weight: 600;
}

/* selectbox 選項 */
div[data-testid="stSidebarContent"] .stSelectbox div[data-baseweb="select"] * {
    font-size: 20px !important;
}

</style>
""", unsafe_allow_html=True)


# -------------------------
# Sidebar 標題底線
# -------------------------

st.markdown("""
<style>

/* radio 標題底線 */
div[data-testid="stSidebarContent"] .stRadio > label {
    position: relative;
    padding-bottom: 7px;
    display: inline-block;
    font-weight: 600;
}

div[data-testid="stSidebarContent"] .stRadio > label::after {
    content: "";
    position: absolute;
    left: 0;
    bottom: 0;
    width: 100px;
    height: 3px;
    background-color: white;
    opacity: 0.9;
}

/* selectbox 標題底線 */
div[data-testid="stSidebarContent"] .stSelectbox > label {
    position: relative;
    padding-bottom: 8px;
    display: inline-block;
    font-weight: 600;
}

div[data-testid="stSidebarContent"] .stSelectbox > label::after {
    content: "";
    position: absolute;
    left: 0;
    bottom: 0;
    width: 100px;
    height: 3px;
    background-color: white;
    opacity: 0.9;
}

</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------
# 初始化 session_state
# -----------------------------------------------------
if "page" not in st.session_state:
    st.session_state.page = "夜市行人地獄(?)"

# -----------------------------------------------------
# 左側欄選單
# -----------------------------------------------------
page = st.sidebar.radio(
    "選擇章節",
    ["夜市行人地獄(?)", "夜市老實說", "行人看這裡", "政府幫幫忙", "政策來檢驗", "AI 小幫手"]
)
st.session_state.page = page

# -----------------------------------------------------
# Routing
# -----------------------------------------------------
if page == "夜市行人地獄(?)":
    render_home()
elif page == "夜市老實說":
    act2_render()
elif page == "行人看這裡":
    act3_render()
elif page == "政府幫幫忙":
    act4_render()
elif page == "政策來檢驗":
    act5_render()
elif page == "AI 小幫手":
    act6_render(page)