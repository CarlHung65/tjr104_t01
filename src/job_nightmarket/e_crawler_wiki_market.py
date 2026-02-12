import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import os

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.google.com/",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}

def random_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))

def fetch_with_retry(url, headers, max_retries=3):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                return resp.text
        except Exception as e:
            print(f"連線失敗：{e}，重試中...")
        random_delay(2, 5)
    raise Exception("多次嘗試後仍無法取得網頁")

regions = {
    "北部": ["臺北市", "新北市", "基隆市", "桃園市", "新竹市", "新竹縣"],
    "中部": ["臺中市", "彰化縣", "南投縣", "雲林縣", "苗栗縣"],
    "南部": ["臺南市", "高雄市", "屏東縣", "嘉義市", "嘉義縣"],
    "東部": ["花蓮縣", "臺東縣", "宜蘭縣"],
    "離島": ["澎湖縣", "金門縣", "連江縣"]
}

URL = "https://zh.wikipedia.org/zh-tw/%E8%87%BA%E7%81%A3%E5%A4%9C%E5%B8%82%E5%88%97%E8%A1%A8"
html = fetch_with_retry(URL, HEADERS)
soup = BeautifulSoup(html, "html.parser")

results = []

for region, cities in regions.items():
    for city in cities:
        h3 = soup.find(lambda tag: tag.name == "h3" and city in tag.get_text())
        if not h3:
            continue
        table = h3.find_next("table", class_="wikitable")
        if not table:
            continue
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if not cols:
                continue
            night_market = cols[0].get_text(strip=True)
            if night_market:
                results.append([region, city, night_market])
        random_delay(0.5, 1.5)

# -------- 新增：自動建立資料夾並輸出 CSV --------
output_dir = r".\Data_raw"
output_path = os.path.join(output_dir, "night_markets.csv")

os.makedirs(output_dir, exist_ok=True)

with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["region", "city", "night_market"])
    writer.writerows(results)

print(f"已產生 CSV：{output_path}")

# 【 e_crawler_wiki_market V1.1版 】

# / 程式要點

# 使用 Requests 搭配 Header 模擬真實瀏覽器
# 設定 User-Agent、Referer 等避免被目標網站拒絕訪問。
# 加入隨機延遲 random_delay()
# 避免爬蟲發送過於頻繁的請求，降低被封鎖的風險。
# 具備重試機制 fetch_with_retry()
# 若連線失敗會重試最多 3 次，每次間隔隨機延遲。
# 使用 BeautifulSoup 解析 Wikipedia 夜市列表頁面
# 依序尋找各縣市對應的 <h3>，再尋找下一個 wikitable 解析資料。
# 以字典定義臺灣各區域與縣市清單
# 加速定位資料表格，不需依賴結構模糊的 HTML。
# 整理資料成 results 列表
# 每筆資料包含：
# 區域（北部、中部、南部、東部、離島）
# 縣市
# 夜市名稱
# 自動建立輸出資料夾（os.makedirs）
# 確保儲存 CSV 前，路徑已建立（不存在則自動建立）。
# 將結果輸出成 night_markets.csv（UTF-8 BOM）
# BOM 使 Excel 在 Windows 下能正常顯示中文。

# / 程式功能

# 自動爬取 Wikipedia「臺灣夜市列表」頁面
# 解析各縣市的夜市名稱並自動分類區域
# 避免爬蟲高頻率訪問而被封鎖（隨機延遲機制）
# 防止連線失敗導致程式中斷（重試機制）
# 自動建立資料夾並將 CSV 寫入指定路徑
# 輸出包含 3 欄位的 CSV：
# region（區域）
# city（縣市）
# night_market（夜市名稱）