import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv

# 讀取 .env
load_dotenv()
API_KEY = os.getenv("Place_api")

# 檔案路徑
CSV_FILE = r"./src/job_nightmarket/Data_raw/night_markets.csv"
RAW_DIR = r"./src/job_nightmarket/Data_raw"
RAW_FILE = os.path.join(RAW_DIR, "market_api.json")

def search_place_id(place_name):
    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "input": place_name,
        "inputtype": "textquery",
        "fields": "place_id",
        "language": "zh-TW",
        "key": API_KEY
    }
    response = requests.get(base_url, params=params)
    data = response.json()
    if data.get("candidates"):
        return data["candidates"][0]["place_id"]
    return None

def get_place_details(place_id):
    base_url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,rating,formatted_address,opening_hours,url,geometry",
        "language": "zh-TW",
        "key": API_KEY
    }
    response = requests.get(base_url, params=params)
    return response.json()

def convert_to_df(details):
    result = details.get("result", {})
    return {
        "名稱": result.get("name"),
        "評分": result.get("rating"),
        "地址": result.get("formatted_address"),
        "緯度": result.get("geometry", {}).get("location", {}).get("lat"),
        "經度": result.get("geometry", {}).get("location", {}).get("lng"),
        "營業時間": "; ".join(result.get("opening_hours", {}).get("weekday_text", [])),
        "Google 地圖連結": result.get("url"),
    }

def main():
    if not API_KEY:
        print("❌ 找不到 API 金鑰！請確認 .env 檔案內已設定 Place_api=YOUR_KEY")
        return

    # 讀取 CSV，取得所有夜市名稱
    df_markets = pd.read_csv(CSV_FILE)
    night_markets = df_markets['night_market'].dropna().unique()

    all_details_json = []  # 用來儲存所有夜市的原始 details
    all_details_df = []    # 用來儲存 DataFrame 預覽資料

    for market in night_markets:
        print(f"🔍 正在查詢：{market} ...")
        place_id = search_place_id(market)
        if not place_id:
            print(f"❌ 找不到 {market} 的地點 ID")
            continue

        details = get_place_details(place_id)
        all_details_json.append(details)
        all_details_df.append(convert_to_df(details))

    # 合併儲存所有夜市 details 到同一個 JSON
    os.makedirs(os.path.dirname(RAW_FILE), exist_ok=True)
    with open(RAW_FILE, "w", encoding="utf-8") as f:
        json.dump(all_details_json, f, ensure_ascii=False, indent=4)
    print(f"📁 全部夜市 JSON 已成功輸出到：{RAW_FILE}")

    # 整理成 DataFrame 預覽
    df_all = pd.DataFrame(all_details_df)
    print("\n===== 全台夜市 DataFrame 預覽（中文） =====")
    print(df_all)

if __name__ == "__main__":
    main()

# 【 e_crawler_market_inform V1.1版 】

# / 程式要點

# 從 .env 讀取 Google Place API 金鑰。
# 讀取夜市清單的 CSV 檔案。
# 使用 Google API 查詢每個夜市的 Place ID。
# 再以 Place ID 取得夜市的詳細資訊。
# 整理資訊為 DataFrame 格式。
# 輸出完整 JSON 與在終端顯示 DataFrame 預覽。

# / 程式功能性

# 自動查詢夜市地點資料：利用 Google Places API 找出 Place ID 與詳細資料。
# 資料清洗與轉換：將 API 結果整理成可用的欄位（名稱 / 評分 / 地址 / 座標 / 營業時間等）。
# 資料輸出：
# 儲存所有夜市的原始 JSON。
# 用 pandas 生成 DataFrame 供檢視與後續分析。