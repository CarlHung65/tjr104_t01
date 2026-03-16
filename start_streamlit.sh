#!/bin/bash
# 1. 進入專案目錄
cd /home/tjr104_t01
# 2. 啟動虛擬環境
source venv/bin/activate
# 3. 在背景啟動 Streamlit 並將日誌輸出到檔案，指定使用 8502 埠號
# 使用 nohup 確保組員關閉終端機後，程式不會中斷執行
nohup streamlit run r_app.py --server.port 8502 > streamlit_run.log 2>&1 &
echo "------------------------------------------------"
echo "Streamlit 前端正在背景啟動中..."
VM_IP=$(curl -s ifconfig.me)
echo "✅ 請透過此網址存取：http://$VM_IP:8502"
echo "📄 啟動日誌請查看：streamlit_run.log"
echo "------------------------------------------------"