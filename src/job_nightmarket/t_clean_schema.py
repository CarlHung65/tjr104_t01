import os
import json
import pandas as pd
import re

REGION_MAP = {
    "北部": ["臺北市", "台北市", "新北市", "基隆市", "桃園市", "新竹市", "新竹縣"],
    "中部": ["臺中市", "台中市", "彰化縣", "南投縣", "雲林縣", "苗栗縣"],
    "南部": ["臺南市", "台南市", "高雄市", "屏東縣", "嘉義市", "嘉義縣"],
    "東部": ["花蓮縣", "臺東縣", "台東縣", "宜蘭縣"],
    "離島": ["澎湖縣", "金門縣", "連江縣"]
}

def get_region(formatted_address: str) -> str:
    for region, cities in REGION_MAP.items():
        for city in cities:
            if city in formatted_address:
                return region
    return "未分類"

def extract_city(formatted_address):
    match = re.search(
        r'(臺北市|台北市|新北市|基隆市|桃園市|新竹市|新竹縣|臺中市|台中市|彰化縣|南投縣|雲林縣|苗栗縣|臺南市|台南市|高雄市|屏東縣|嘉義市|嘉義縣|花蓮縣|臺東縣|台東縣|宜蘭縣|澎湖縣|金門縣|連江縣)',
        formatted_address
    )
    return match.group(1) if match else ""

def extract_area_road(formatted_address):
    city = extract_city(formatted_address)
    if city:
        idx = formatted_address.find(city)
        return formatted_address[idx + len(city):].strip()
    return ""

def extract_zipcode(formatted_address):
    match = re.search(r'(\d+)', formatted_address)
    return match.group(1) if match else ""

def fix_time_format(x):
    if not x:
        return ""
    x = str(x).zfill(4)
    return f"{x[:2]}:{x[2:]}"

def clean_market_name(name: str) -> str:
    patterns = [
        r"(.*?夜市)",
        r"(.*?商圈)",
    ]
    for pattern in patterns:
        match = re.match(pattern, name)
        if match:
            return match.group(1)
    return name.split()[0]

def load_zipcode_area_map(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    zipcode_to_area = {}
    for city_entry in data:
        for district in city_entry["districts"]:
            zip_code = district["zip"]
            area_name = district["name"]
            zipcode_to_area[zip_code] = area_name
    return zipcode_to_area


# ======================================================
#  ★ 主邏輯：每夜市只輸出一行 + 無 opening_hours 欄位
# ======================================================
def normalize_nightmarket_list(api_data: list, zipcode_to_area: dict) -> pd.DataFrame:

    normalize_name_map = {
        "饒河街 觀光夜市": "饒河街觀光夜市",
        "Iranmeylek Night Market": "東清夜市",
    }

    zipcode_overrides = {
        "四腳亭吉安宮夜市": "224",
        "內惟夜市": "804",
        "汴頭夜市": "606",
        "梧棲中港觀光夜市": "435",
        "鳳山中山路夜市": "830",
        "安西夜市": "722"
    }

    area_overrides = {
        "楊梅觀光夜市": "楊梅區金山街263號",
        "線西星期一夜市": "線西鄉",
        "卓蘭觀光夜市": "卓蘭鎮卓蘭鎮",
        "鳳山中山路夜市": "鳳山區中山路102號",
        "汴頭夜市": "阿里山公路中埔鄉嘉義縣18號",
        "小北新成功夜市": "北區西門路四段171巷2號",
        "斗六成功夜市": "雲林縣斗六市成功二街",
        "竹中夜市": "竹東鎮",
        "溪州夜市（第二代）": "溪州鄉中山路三段736號",
    }

    exclude_list = {
        "康新聯合診所",
        "馬港大眾飲食店",
        "香蒜烏龍豆干",
        "水裡港福順宮朱李池三府王爺",
        "埤頭公園",
        "竹圍夜市牛排（淡水竹圍晚餐、宵夜）",
        "斗六門當归鴨 霧峰店",
        "龍山蚵仔煎",
        "士東市場",
        "五股地瓜球 星期三夜市",
        "遠東SOGO 台北忠孝館",
        "遠東SOGO",
    }

    # ★ 內部儲存（不輸出 opening_hours）
    records = {}

    for item in api_data:
        result = item.get("result", {})
        if not result:
            continue

        raw_name = result.get("name", "")
        name = normalize_name_map.get(raw_name, raw_name)
        if name not in normalize_name_map.values():
            name = clean_market_name(name)

        if name in exclude_list:
            continue

        # ★★★ 避免夜市重複（同名夜市只保留第一筆）★★★
        if name in records:
            continue

        formatted_address = result.get("formatted_address", "")

        nightmarket_zipcode = zipcode_overrides.get(
            name, extract_zipcode(formatted_address)
        )
        nightmarket_area_road = area_overrides.get(
            name, extract_area_road(formatted_address)
        )

        lat = result.get("geometry", {}).get("location", {}).get("lat", "")
        lng = result.get("geometry", {}).get("location", {}).get("lng", "")

        viewport = result.get("geometry", {}).get("viewport", {})
        ne = viewport.get("northeast", {})
        sw = viewport.get("southwest", {})

        url = result.get("url", "")
        periods = result.get("opening_hours", {}).get("periods", [])
        nightmarket_rating = result.get("rating", None)

        nightmarket_region = get_region(formatted_address)
        nightmarket_city = extract_city(formatted_address)
        nightmarket_id = str(url.split("cid=")[-1]) if "cid=" in url else ""

        zip_code = str(nightmarket_zipcode)
        nightmarket_zipcode_name = zipcode_to_area.get(zip_code, "")

        # ◆ 沒有營業時間 → 不輸出
        if not periods:
            continue

        # ◆ 初始化（此區塊不會再被重複執行）
        records[name] = {
            "nightmarket_id": nightmarket_id,
            "nightmarket_name": name,
            "nightmarket_latitude": lat,
            "nightmarket_longitude": lng,
            "nightmarket_area_road": nightmarket_area_road,
            "nightmarket_zipcode": nightmarket_zipcode,
            "nightmarket_zipcode_name": nightmarket_zipcode_name,
            "nightmarket_rating": nightmarket_rating,
            "nightmarket_region": nightmarket_region,
            "nightmarket_city": nightmarket_city,
            "nightmarket_url": url,
            "nightmarket_northeast_latitude": ne.get("lat", ""),
            "nightmarket_northeast_longitude": ne.get("lng", ""),
            "nightmarket_southwest_latitude": sw.get("lat", ""),
            "nightmarket_southwest_longitude": sw.get("lng", ""),
            "_opening_hours": {}
        }

        # ★ opening_hours 處理完全照原始邏輯
        for p in periods:
            open_raw = p["open"]["time"]

            if open_raw == "0000":
                for wd in range(7):
                    records[name]["_opening_hours"][wd] = {
                        "open": "00:00",
                        "close": "23:59"
                    }
                continue

            wd = p["open"]["day"]
            close_raw = p["close"]["time"] if "close" in p else None

            open_t = fix_time_format(open_raw)
            close_t = fix_time_format(close_raw) if close_raw else ""

            records[name]["_opening_hours"][wd] = {
                "open": open_t,
                "close": close_t
            }

    # ======================================================
    #        ★ 組裝最終輸出：不含 opening_hours 欄位
    # ======================================================
    rows = []

    for name, info in records.items():

        if not info["_opening_hours"]:
            continue

        parts = []
        for wd in range(7):
            if wd in info["_opening_hours"]:
                o = info["_opening_hours"][wd]
                parts.append(
                    f"\"weekday\": {wd}, \"open\": \"{o['open']}\", \"close\": \"{o['close']}\""
                )

        opening_hours_str = " / ".join(parts)

        row = info.copy()
        del row["_opening_hours"]
        row["nightmarket_opening_hours"] = opening_hours_str

        rows.append(row)

    return pd.DataFrame(rows)


def export_csv(df: pd.DataFrame, filename="nightmarket_clean.csv"):
    output_dir = r".\Data_clean"
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, filename)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"CSV 已成功輸出至：{csv_path}")


def main():
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(base_dir, "Data_raw", "market_api.json")
        print(f"正在讀取：{json_path}")

        with open(json_path, "r", encoding="utf-8") as f:
            api_data = json.load(f)

        zipcode_json_path = os.path.join(base_dir, "Data_raw", "zipcode.json")
        zipcode_to_area = load_zipcode_area_map(zipcode_json_path)

        df = normalize_nightmarket_list(api_data, zipcode_to_area)

        df["nightmarket_zipcode"] = df["nightmarket_zipcode"].astype(int)
        df = df.sort_values(["nightmarket_zipcode", "nightmarket_name"])

        export_csv(df)

    except Exception as e:
        print(f"發生錯誤：{e}")


if __name__ == "__main__":
    main()


# 【 t_clean_schema_merge V1.1版 】

# / 程式要點

# 讀取 Google API 夜市原始資料（JSON）
# 從 market_api.json 取得每個夜市的資訊。
# 整理夜市基本資訊
# 標準化夜市名稱（處理錯字、英文名稱等）
# 過濾非夜市資料（根據排除清單 exclude_list）
# 自動解析地址資訊：
# 縣市
# 詳細路段
# 郵遞區號
# 對應行政區名稱（zipcode.json）
# 補強錯誤或異常資料
# 部分夜市使用覆寫郵遞區號（zipcode_overrides）
# 或覆寫地址（area_overrides）
# 只保留每個夜市一筆完整資料
# 若同一夜市在 API 回傳多筆 → 自動跳過後續重複項
# 依夜市名稱（name）判斷，不重複輸出
# 處理營業時間（opening_hours）
# 不直接輸出每日資料，而是合併為一個字串欄位
# 支援 24 小時格式（0000 → 00:00~23:59）
# 支援多組時段合併至 _opening_hours 暫存區

# / 功能性說明

# 1. 夜市資料正規化
# 統一格式化夜市名稱，修正 Google Places 的常見錯誤名稱。
# 過濾不是夜市的資料，比如：
# 2. 解析完整地址資訊
# 程式會自動從 API 地址解析出以下欄位：
# nightmarket_region（北中南東離島）
# nightmarket_city（縣市）
# nightmarket_area_road（區／路段）
# nightmarket_zipcode（郵遞區號）
# nightmarket_zipcode_name（行政區）
# 3. 地理資訊整理
# # 緯度（latitude）
# 經度（longitude）
# 右上角與左下角座標（viewport）
# 這些通常可用於前端地圖呈現。
# 4. 自動排除重複夜市
# 5. 營業時間整合
# API 的 periods 通常是：
# 每日一段或多段
# 或出現 24 小時資料
# 程式會統整所有星期的時間資訊為一個欄位：
# nightmarket_opening_hours