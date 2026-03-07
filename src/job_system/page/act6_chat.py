import streamlit as st
import ollama
import requests
from summary import build_context

SYSTEM_PROMPT = """
你是本交通事故地圖專案的 AI 小幫手，請使用自然、流暢、像真人的繁體中文回答。

【回答原則】
你只能根據「context 中提供的所有資訊」回答問題。
只要資訊有出現在 context 中，你都可以明確使用。
如果使用者的問題沒有指定夜市名稱，而 context 中有 selected_market，
請自動使用 selected_market 作為回答的夜市。

【可回答的內容】
你可以回答 context 中包含的資訊，例如：
- 任一夜市的名稱
- 任一夜市的事故數、PDI、危險等級
- 任一夜市的 Google 評分
- 章節摘要（summary）
- 其他 context 中明確出現的資訊

【無資料時的回覆】
如果使用者詢問的內容不在 context 中，請回答：
「目前頁面中沒有相關資料喔！」

【使用者問「我能問什麼？」時】
請根據 context，自動整理出「目前可以查詢的項目」，並用自然語氣回答。
"""

def check_ollama():
    try:
        r = requests.get("http://localhost:11434/api/tags", timeout=2)
        return r.status_code == 200
    except:
        return False


def act6_render(page):

    if st.session_state.page != "AI 小幫手":
        return

    if "ai_step" not in st.session_state:
        st.session_state["ai_step"] = 1

    chapter_map = {
        "夜市行人地獄 (?)": 1,
        "夜市老實說": 2,
        "行人看這裡": 3,
        "政府幫幫忙": 4
    }

    # -----------------------------------------------------
    # ⭐ 讀取章節內容
    # -----------------------------------------------------
    current_chapter = chapter_map.get(page, None)

    ch1 = st.session_state.get("ch1", None)
    ch2 = st.session_state.get("ch2", None)
    ch3 = st.session_state.get("ch3", None)
    ch4 = st.session_state.get("ch4", None)

    # -----------------------------------------------------
    # ⭐ 建立 context（合併 1～4 章）
    # -----------------------------------------------------
    context_parts = []

    if ch1 is not None:
        context_parts.append(
            build_context(current_chapter=1, **ch1)
        )

    if ch2 is not None:
        context_parts.append(
            build_context(current_chapter=2, **ch2)
        )

    if ch3 is not None:
        context_parts.append(
            build_context(current_chapter=3, **ch3)
        )

    if ch4 is not None:
        context_parts.append(
            build_context(current_chapter=4, **ch4)
        )

    # ⭐ 最終 context（合併所有章節）
    if context_parts:
        context_text = "\n\n".join(context_parts)
    else:
        context_text = "（目前沒有可提供的 context 資料）"

    # -----------------------------------------------------
    # ⭐ UI 設定
    # -----------------------------------------------------

    st.title("我有話要問｜先選想查夜市")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    if "system_cached" not in st.session_state:
        st.session_state.system_cached = [{"role": "system", "content": SYSTEM_PROMPT}]

    st.markdown("""
    <hr style="
        border: 0;
        height: 6px;
        background: linear-gradient(90deg, #e53935, #ff8a80);
        border-radius: 3px;
    ">
    """, unsafe_allow_html=True)

    st.markdown("""
    <style>
    div[data-testid="stChatMessage"] p {
        font-size: 22px !important;
        line-height: 1.7 !important;
    }
    div[data-testid="stChatMessage"] pre {
        font-size: 20px !important;
    }
    div[data-testid="stChatInput"] textarea {
        font-size: 25px !important;
    }
    div[data-testid="stChatInput"] textarea::placeholder {
        font-size: 25px !important;
    }
    </style>
    """, unsafe_allow_html=True)

    if not check_ollama():
        st.error("⚠️ 無法連線到 Ollama。請先啟動 Ollama，然後重新整理頁面。")
        st.stop()

    # -----------------------------------------------------
    # ⭐ 使用者輸入
    # -----------------------------------------------------
    user_input = st.chat_input("輸入訊息...")

    if not user_input:
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                st.write(msg["content"])
        return

    st.session_state.messages.append({"role": "user", "content": user_input})

    with st.chat_message("user"):
        st.write(user_input)

    # ⭐ 只取最後一個 user 問題（避免覆蓋 context）
    last_user_question = st.session_state.messages[-1]

    # -----------------------------------------------------
    # ⭐ context（資料）→ user 角色（不能用 system）
    # -----------------------------------------------------
    context_prompt = {
        "role": "user",
        "content": f"以下是你可以引用的所有資訊（context）：\n{context_text}"
    }

    # -----------------------------------------------------
    # ⭐ context 說明 → assistant 角色
    # -----------------------------------------------------
    context_notice = {
        "role": "assistant",
        "content": "以上內容是你可以用來回答問題的資料。"
    }

    # -----------------------------------------------------
    # ⭐ 正確 messages 順序（最重要）
    # -----------------------------------------------------
    messages = (
        st.session_state.system_cached +   # system（規則）
        [context_notice] +                 # assistant（說明）
        [context_prompt] +                 # user（context）
        [last_user_question]               # user（真正的問題）
    )

    # -----------------------------------------------------
    # ⭐ 呼叫 LLM
    # -----------------------------------------------------
    ai_msg = ""

    with st.chat_message("assistant"):
        placeholder = st.empty()

        for chunk in ollama.chat(
            model="llama3.2:1b",
            messages=messages,
            stream=True
        ):
            token = chunk["message"]["content"]
            ai_msg += token
            placeholder.write(ai_msg)

    st.session_state.messages.append({"role": "assistant", "content": ai_msg})
    if st.session_state["ai_step"] == 5:
        st.session_state["ai_step"] = 1
    else:
        st.session_state["ai_step"] += 1

