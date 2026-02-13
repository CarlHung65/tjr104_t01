import pandas as pd
from datetime import timedelta
import time
import random
import io
import sys
from airflow.sdk import task
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook


# 1. 先確保路徑進去了
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# 2. 在 sys.path 之後才進行 import
from utils.get_table_from_mysql_gcp import get_table_from_sqlserver
from utils.request_weather_api import request_weather_api


"""========================定義TASK 1========================"""


@task(retries=2, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(hours=2))
def e_get_uniq_acc_geo(target_year: int) -> pd.DataFrame:
    """
    Extract: 提取資料源 - 讀取車禍資料表並取得去重後的經緯度
    """
    query = f"""SELECT year(accident_datetime) as accident_year,
                           longitude, latitude
                        FROM accident_sq1_main 
                            GROUP BY  longitude, latitude, 
                                    year(accident_datetime)
                                    having accident_year = {target_year}"""
    df_acc = get_table_from_sqlserver("test_accident", query)

    # 截取年
    df_acc["accident_year"] = pd.to_datetime(
        df_acc["accident_year"], errors='coerce', format="%Y").dt.year

    # 在WGS84座標系下，經緯度差0.01度大約相當於緯度方向1110公尺、經度方向約1000公尺。
    # 一般天氣預報模型網格解析度為1~20公里，0.01度差異(約1公里)在同一網格內，對預報影響很小。
    # 所以將經緯度統一進位到小數點後2位，減少送請求次數與後續要處理的資料量。
    df_acc["lat_round"] = df_acc["latitude"].astype("float64").round(2)
    df_acc["lon_round"] = df_acc["longitude"].astype("float64").round(2)

    df_uniq_loc_acc = df_acc.drop_duplicates(['lat_round', 'lon_round'])
    print(
        f"Year {target_year}: Found {len(df_uniq_loc_acc)} unique locations.")

    # a df with [accident_year, longitude, latitude, lat_round, lon_round]
    return df_uniq_loc_acc


"""========================定義TASK 2========================"""


@task
def prep_batch_plan(df_unique_loc_acc: pd.DataFrame, target_year: int,
                    batch_size=50) -> list[pd.DataFrame]:
    # batch_size為defalut設定批次API請求，一次請求取50個地點

    # 預備存parquet路徑(直接存到GCS上)
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')  # 初始化
    bucket_name = 'tjr104-01_weather'
    save_dir = f"weather_cache/{target_year}"

    # 提取一個年度要搜尋的地點
    df_unique_loc_y = df_unique_loc_acc.copy()

    # 查詢目前save_dir有幾份檔.parquet，存成set，可以減少時間複雜度
    existing_files = {f.name for f in save_dir.glob("*.parquet")}

    # 組合出一個series，描述各資料列對應的檔案名稱，並增加為新的一欄
    df_unique_loc_y["file_name"] = (
        df_unique_loc_y["lat_round"].astype(str).str.replace(".", "-", regex=False) + "_" +
        df_unique_loc_y["lon_round"].astype(str).str.replace(".", "-", regex=False) +
        ".parquet")

    # 用~排除同名檔案
    df_needed = df_unique_loc_y[~df_unique_loc_y["file_name"].isin(
        existing_files)]

    # 丟棄accident_year、longitude、latitude這三欄，避免造成Returned value、x-com過長
    df_needed = df_needed.drop(
        columns=["accident_year", "longitude", "latitude"])

    # 利用for-loop切分出每一次請求下的請求計畫(batch plan)，內容包含：經緯度與檔名。
    batch_plan = []

    for i in range(0, len(df_needed), batch_size):
        df_a_batch = df_needed.iloc[i: i + batch_size]
        # 之前這裡用dict描述df_a_batch，會導致return value (batch_plan)過長，所以這裡改成用dataframe物件
        batch_plan.append(df_a_batch)

    print(f"資料夾已存有{len(existing_files)}個地點對應的檔案，故尚須處理{len(df_needed)}個地點。")
    print(f"總結切分出{len(batch_plan)}份批數，分批請求API")

    return batch_plan


"""========================定義TASK 3========================"""


@task(
    pool="weather_api_pool",  # 需到UI進一步給值，指一次可以執行多少個同類task
    retries=3,  # 如果出現except，最多再重試3次，總計task group中，每個task可跑4次
    retry_delay=timedelta(minutes=20),  # 20分鐘後才重試
    retry_exponential_backoff=True,  # 讓等待時間隨次數增加(指數退避)
    max_retry_delay=timedelta(hours=2),  # 指數退避下，最長間隔2小時後重試
    # 排除等待時間，如果執行總時間超過2小時，殺掉該task避免佔用pool資源
    execution_timeout=timedelta(hours=2),
    do_xcom_push=False  # 回傳的xcom不推送到下一個task，省掉存xcom的記憶體空間
)
def e_crawler_weatherapi(df_one_batch: pd.DataFrame,
                         target_year: int) -> str | None:

    # 預備存parquet路徑(直接存到GCS上)
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')  # 初始化
    bucket_name = 'tjr104-01_weather'
    save_dir = f"weather_cache/{target_year}"

    # 遍歷df_one_batch，把經緯度的值分別組合出一個字串
    lat_round_lst = [str(lat_num) for lat_num in df_one_batch["lat_round"]]
    lon_round_lst = [str(lon_num) for lon_num in df_one_batch["lon_round"]]
    lats_str = ",".join(lat_round_lst)
    lons_str = ",".join(lon_round_lst)

    # 準備要拋給API的param，一次針對10個經緯地點，抓一年份數據，中間會逐點存檔，但整體而言這樣算一次請求
    start_date = f"{target_year}-01-01"
    end_date = f"{target_year}-12-31"
    vars = ["temperature_2m", "apparent_temperature", "rain",
            "showers", "precipitation", "visibility", "weather_code"]
    col_name = ['datetime_ISO8601', 'temperature_2m_degree',
                'apparent_temperature_degree', 'rain_mm',
                'showers_mm', 'precipitation_mm',
                'visibility_m', 'weather_code',
                'latitude_round', 'longitude_round']

    # calling API
    data = request_weather_api(lats_str, lons_str, start_date, end_date, vars)

    # 簡易清洗後就存成parquet，備份資料源。
    if data:
        data = data if isinstance(data, list) else [data]
        for j in range(0, len(data)):  # len(data) == len(lat_round_lst)
            if "hourly" in data[j]:  # data[j] is a dict of one certain location
                # data[j]["hourly"] is also a dict
                hourly_df_a_loc = pd.DataFrame(data[j]['hourly'])

                # 補回lat_round、lon_round，以便後續追溯這筆天氣資料是靠近哪一個事故地經緯度。
                hourly_df_a_loc['latitude_round'] = float(lat_round_lst[j])
                hourly_df_a_loc['longitude_round'] = float(lon_round_lst[j])

                # 置換成想要的欄位名稱
                hourly_df_a_loc.columns = col_name
                # 定義存檔名稱
                lat_s = str(lat_round_lst[j]).replace(".", "-")
                lon_s = str(lon_round_lst[j]).replace(".", "-")
                file_name = f"{save_dir}/{lat_s}_{lon_s}.parquet"

                # 存成 Parquet 較節省空間(直接存到GCS上)，印出進度

                try:
                    print(f"正在處理: {(j+1)}/{len(data)} file")

                    # 最穩定的 (io.BytesIO + engine='pyarrow')可以避開 pandas內部讀寫GCS可能產生的版本衝突
                    buffer = io.BytesIO()
                    hourly_df_a_loc.to_parquet(
                        buffer, index=False, engine='pyarrow')
                    buffer.seek(0)
                    gcs_hook.upload(
                        bucket_name=bucket_name,
                        object_name=file_name,
                        data=buffer.getvalue(),  # parquet資料透過data傳入
                        mime_type='application/octet-stream')
                except Exception as e:
                    print(f"第{j+1}/{len(data)} file寫入失敗! {e}")
                else:
                    print(f"已成功寫入第{j+1}/{len(data)} file: {file_name}!")

    else:
        # 印出前100個字看發生什麼事，印出進度
        print(f"API回傳內容: {str(data)[:100]}")

    # 緩解server負擔
    time.sleep(random.uniform(5, 10))
    return str(bucket_name)
