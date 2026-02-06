from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

"""
預期步驟 - 針對歷史資料:
# 1. 從MySQL server讀取acc_nearby_obs_stn，每個列都代表事件ID對應的發生日期、時間(小時)與最近觀測站record_id
# 2. 利用事件日期、時間(小時)與最近觀測站record_id，到weather_hourly_history資料表找尋是否有存下歷史天氣資料。
# 3. 回傳資料缺失的條件conditions_to_crawling，判定需要爬蟲。
"""


def get_table_from_sqlserver(database: str, dql_text) -> list[list]:
    # 準備與GCP VM上的MySQL server的連線
    load_dotenv()
    username = quote_plus(os.getenv("user"))
    password = quote_plus(os.getenv("passwd"))
    host = quote_plus(os.getenv("host"))
    port = quote_plus(os.getenv("port"))
    charset = quote_plus(os.getenv("charset"))
    db_name = database

    # 如.env沒有mail_adress，以jessie為預設
    writer = quote_plus(os.getenv("mail_address", "jessie"))

    # {host}:{port}應該要是 localhost:3307? 、 127.0.0.1:3307? 、mysql:3307?
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{host}:{port}/{db_name}?charset={charset}",
        echo=False,  # 使用預設值，不印SQL日誌，保持乾淨輸出，生產環境適用
        pool_pre_ping=True,  # 檢查連線有效性
        pool_recycle=300,    # 每5分鐘自動重整連線，可再調整
        connect_args={'connect_timeout': 60})

    try:
        with engine.connect() as conn:
            cursor_result = conn.execute(dql_text)
            result = cursor_result.fetchall()
            return result  # 回傳list of rows
    except Exception as e:
        print(f"讀取Error: {e}")


def e_crawler_weather_hist_01_checkmiss(stn_weather_data: list[list],
                                        relation_btw_stn_acc: list[list]) -> set[tuple] | None:
    """Read the table of accident_nearby_obs_stn, and weather_hourly_history
    from the assigned database in MySQL server (given by engine object).
    Return a list of lists of some accident_id the weather data is missing in 
    the table of weather_hourly_history."""

    # 盤點需要爬蟲的觀測站別＋日期有幾組，並用set去掉重複的爬蟲需求。
    conditions_to_crawling = set()
    for row in relation_btw_stn_acc:  # (date(2024,4,13),17,"467350")
        date_and_record_id = (row[0], row[1])  # (date(2024,4,13),17)
        if date_and_record_id != stn_weather_data:  # (date(2024,4,13),17)
            crawling_stn_id = str(row[2])
            crawling_year = row[0].year
            crawling_month = row[0].month
            crawling_monthday = row[0].day
            conditions_to_crawling.add((crawling_stn_id, crawling_year,
                                        crawling_month, crawling_monthday))

    return conditions_to_crawling


# Step 1:查詢 車禍事故日 & 附近觀測站。回傳list of rows, eg.[(datetime.date(2024, 4, 13), 17, "467350")]
relation_btw_stn_acc = get_table_from_sqlserver("test_weather",
                                                text("""SELECT DATE(accident_datetime), station_record_id, 
                                                               station_id
                                                            FROM accident_nearby_obs_stn ANS
                                                                GROUP BY DATE(accident_datetime), 
                                                                        station_record_id, station_id;
                                                         """))

# Step 2: 查詢氣象歷史資料
stn_weather_data = get_table_from_sqlserver("test_weather",
                                            text("""SELECT DATE(observation_datetime), station_record_id
                                                            FROM weather_hourly_history
                                                                GROUP BY DATE(observation_datetime), station_record_id;
                                                     """))

# Step 3: 主程式: 比對step1&step2兩張表，比對是否存有事故附近觀測站當日的天氣資料。
conditions_to_crawling = e_crawler_weather_hist_01_checkmiss(
    stn_weather_data, relation_btw_stn_acc)

# if __name__ == "__main__":
#     # 測試區：
#     print(conditions_to_crawling)
#     print(len(conditions_to_crawling))
