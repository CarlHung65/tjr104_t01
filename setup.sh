#!/bin/bash
# 讓系統遇到錯誤就停止
set -e

echo " 開始初始化 Streamlit 前端環境..."

# 1. 建立虛擬環境 (如果不存在的話)
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "虛擬環境 venv 建立完成"
fi

# 2. 安裝套件
echo "📦 正在安裝必要套件，請稍候..."
./venv/bin/pip install streamlit pandas sqlalchemy pymysql redis python-dotenv groq streamlit-folium plotly altair polyline requests

# 3. 自動判斷 Mac 的 .zshrc 或 Linux 的 .bashrc
CONF_FILE="$HOME/.zshrc"
if [ ! -f "$CONF_FILE" ]; then CONF_FILE="$HOME/.bashrc"; fi

if ! grep -q "st-start" "$CONF_FILE"; then
    echo "alias st-start='$(pwd)/start_streamlit.sh'" >> "$CONF_FILE"
    echo "已將 st-start 加入您的別名清單 ($CONF_FILE)"
fi

echo "------------------------------------------------"
echo "初始化成功！請輸入 'source ~/.bashrc' 讓設定生效。"
echo "之後您只需要輸入 'st-start' 就能啟動前端了！"
echo "------------------------------------------------"