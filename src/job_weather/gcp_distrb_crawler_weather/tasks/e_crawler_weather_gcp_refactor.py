import pandas as pd
from datetime import timedelta
import pendulum
import time
import random
import io
import sys
from pathlib import Path
from airflow.sdk import task
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到 ./utils下的模組或套件
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# 2. 在sys.path之後才進行import
from utils.get_table_from_mysql_gcp import get_table_from_sqlserver
from utils.create_view_table import create_view_table
from utils.request_weather_api import request_weather_api


"""========================定義TASK 1========================"""


@task(retries=3, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(minutes=30))
def e_get_uniq_acc_geo(target_year: int, database: str) -> pd.DataFrame:
    """
        Extract: 從MySQL server讀取車禍資料主表，並取得進位＋去重後的經緯度組合

        :param target_year: 要從MySQL資料表查詢哪一年份的車禍資料主表
        :type target_year: int
        :param database: 要從MySQL哪一個資料庫查詢target_year車禍資料主表
        :type database: str
        :return: 將經緯度都進位至小數點後二位，再去掉重複出現的經緯度組合之後的pandas DataFrame
        :rtype: DataFrame
    """

    # 1. 指派要查詢的資料表名稱
    this_year = pendulum.now().year
    table_name = "accident_sq1_main" if target_year < this_year else "accident_new_sq1_main"

    # 2. 撰寫DQL語句
    query = f"""SELECT accident_id,
                        TIMESTAMP(date(accident_datetime),
                                    SEC_TO_TIME(ROUND(
                                                        TIME_TO_SEC(
                                                            time(accident_datetime)) / 3600
                                                        ) * 3600
                                                )
                                  ) as approx_accident_datetime,
                        accident_datetime as actual_accident_datetime, longitude, latitude,
                        round(longitude, 2) as longitude_round, round(latitude, 2) as latitude_round
                    FROM {table_name}
                        GROUP BY longitude, latitude, accident_id, accident_datetime
                                having year(accident_datetime) = {target_year};
            """

    # 3. 從MySQL server取得資料表
    df_acc = get_table_from_sqlserver(database, query)

    # 4. 同時建立view表，以利未來將accident與weather data兩張表做關聯
    view_ddl = f"""CREATE OR REPLACE VIEW v_acc_approx_loc_{target_year}
                        AS {query};
                """
    create_view_table(database, view_ddl)

    # 5. 經緯度簡化 - 進位
    # 在WGS84座標系下，經緯度差0.01度大約相當於緯度方向1110公尺、經度方向約1000公尺。
    # 一般天氣預報模型網格解析度為1~20公里，0.01度差異(約1公里)在同一網格內，對預報影響很小。
    # 所以將經緯度統一進位到小數點後2位，減少送請求次數與後續要處理的資料量。
    df_acc["lat_round"] = df_acc["latitude"].astype("float64").round(2)
    df_acc["lon_round"] = df_acc["longitude"].astype("float64").round(2)

    # 6. 進位後如果有相同經緯度組合重複出現，則去重
    df_acc_uniq_loc = df_acc.drop_duplicates(['lat_round', 'lon_round'])

    # 7. 只留'lat_round', 'lon_round'這二欄，避免造成Returned value、x-com過長
    df_acc_uniq_loc = df_acc_uniq_loc.loc[:, ["lat_round", "lon_round"]]
    print(
        f"Year {target_year}: Found {len(df_acc_uniq_loc)} unique locations.")

    # a df with [lat_round, lon_round]
    return df_acc_uniq_loc


"""========================定義TASK 2========================"""


@task
def prep_batch_plan(df_acc_unique_loc: pd.DataFrame, target_year: int,
                    batch_size=50) -> list[pd.DataFrame]:
    """
        將DataFrame: df_acc_unique_loc中的經緯度組合與data warehouse中的天氣觀測資料(e.g. GCS上的.parquet)比對，
        盤點有哪些經緯度組合的年度天氣觀測資料尚未下載並存放於data warehouse。
        過濾並留下有缺失天氣觀測資料的經緯度組合所屬的資料列，然後以50列為一批次單位，切割df_acc_unique_loc資料列。

        :param df_acc_unique_loc: 經過進位與去重而得到的事故地經緯度
        :type df_acc_unique_loc: pd.DataFrame
        :param target_year: 要從data warehouse (e.g. GCS)查詢哪一年份的天氣觀測資料
        :type target_year: int
        :param batch_size: 針對在df_acc_unique_loc裡面發現有缺失天氣觀測資料的資料列，
        以batch_size的資料列切割出不同batch的dataframe，batch_size預設值為50列(50個經緯度組合)
        :return: 裝有多個批次(batch)的dataframes的list，每一元素為一個批次df
        :rtype: list[DataFrame]
    """

    # 1. 組合出一個Series，描述各資料列對應的檔案名稱，並增加為新的一欄
    df_acc_uniq_loc = df_acc_unique_loc.copy()
    df_acc_uniq_loc["file_name"] = (
        df_acc_uniq_loc["lat_round"].astype(str).str.replace(".", "-", regex=False) + "_" +
        df_acc_uniq_loc["lon_round"].astype(str).str.replace(".", "-", regex=False) +
        ".parquet")

    # 2. 宣告在GCS上存parquet的路徑名稱
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')  # 初始化
    bucket_name = 'tjr104-01_weather'
    save_dir = f"weather_cache/{target_year}"

    # 3. 查詢目前GCS的save_dir有幾份檔.parquet，存成set，可以減少時間複雜度至O(1)
    files = gcs_hook.list(bucket_name=bucket_name,
                          prefix=save_dir)
    existing_files = {Path(f).name for f in files if f.endswith(".parquet")}

    # 4. 針對歷史年份，用~排除同名檔案。針對尚未結束的今年，不排除同名檔案。
    this_year = pendulum.now().year
    if target_year < this_year:
        df_needed = df_acc_uniq_loc[~df_acc_uniq_loc["file_name"].isin(
            existing_files)]
    else:
        df_needed = df_acc_uniq_loc.copy()

    # 5. 利用for-loop切分出每一次請求下的請求計畫(batch plan)，內容包含：經緯度與檔名。
    batch_plan = []

    for i in range(0, len(df_needed), batch_size):
        df_a_batch = df_needed.iloc[i: i + batch_size]
        # 之前這裡用dict描述df_a_batch，會導致return value(=batch_plan的值)過長，所以這裡改成用dataframe物件
        batch_plan.append(df_a_batch)

    print(f"資料夾已存有{len(existing_files)}個地點對應的檔案，故尚須處理{len(df_needed)}個地點。")
    print(f"總結切分出{len(batch_plan)}份批數，分批請求API")

    del df_acc_uniq_loc, df_acc_unique_loc
    import gc
    gc.collect()
    return batch_plan


""" == == == == == == == == == == == ==定義TASK 3 == == == == == == == == == == == =="""


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
    """
        Extract: 遍歷引入的df_one_batch中的經緯度資料，向OpenMeteo historical weather API
        請求該地點的某個整年度天氣觀測資料。

        :param df_one_batch: 包含經度與緯度的dataframe
        :type df_one_batch: pd.DataFrame
        :param target_year: 說明向OpenMeteo historical weather API請求哪一年度的天氣觀測資料
        :type target_year: int
        :return: GCS bucke名稱字串，若request API失敗或觸發例外則回傳None
        :rtype: str | None
    """

    # 1. 宣告在GCS上存parquet的路徑名稱
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')  # 初始化
    bucket_name = 'tjr104-01_weather'
    save_dir = f"weather_cache/{target_year}"

    # 2. 遍歷df_one_batch，把經緯度的值分別組合出一個字串
    lat_round_lst = [str(lat_num) for lat_num in df_one_batch["lat_round"]]
    lon_round_lst = [str(lon_num) for lon_num in df_one_batch["lon_round"]]
    lats_str = ",".join(lat_round_lst)
    lons_str = ",".join(lon_round_lst)

    # 3. 準備要拋給API的param，一次針對50個經緯地點，抓一年份數據，中間會逐點存檔，但整體而言這樣算一次請求
    start_date = f"{target_year}-01-01"
    # 如果當年度尚未結束，但end_date設定為yyyy-12-31再作為參數傳入的話，API會回傳None，因此需要彈性指派end_date的值
    today = pendulum.now()
    previous_day = today.subtract(days=3).format("YYYY-MM-DD")
    this_year = today.year
    end_date = f"{target_year}-12-31" if this_year > target_year else f"{previous_day}"

    # 指派要請求什麼氣象指標
    vars = ["temperature_2m", "apparent_temperature", "rain",
            "showers", "precipitation", "visibility", "weather_code"]

    # 4. 預備欄位名稱
    col_name = ['datetime_ISO8601', 'temperature_2m_degree',
                'apparent_temperature_degree', 'rain_mm',
                'showers_mm', 'precipitation_mm',
                'visibility_m', 'weather_code',
                'latitude_round', 'longitude_round']

    # 5. calling API
    data = request_weather_api(lats_str, lons_str, start_date, end_date, vars)

    # 6. 將請求結果data做簡易清洗後就立即存成parquet，備份資料源。
    if data:
        data = data if isinstance(data, list) else [data]
        for j in range(0, len(data)):  # len(data) == len(lat_round_lst)
            if "hourly" in data[j]:  # data[j] is a dict of one certain location
                # data[j]["hourly"] is also a dict
                df_a_loc_hourly = pd.DataFrame(data[j]['hourly'])

                # 補回lat_round、lon_round，以便後續追溯這筆天氣資料是靠近哪一個事故地經緯度。
                df_a_loc_hourly['latitude_round'] = float(lat_round_lst[j])
                df_a_loc_hourly['longitude_round'] = float(lon_round_lst[j])

                # 置換成想要的欄位名稱，但盡可能的跟資料源欄位名稱一樣，此步驟能做到與API隔離就好
                df_a_loc_hourly.columns = col_name

                # 定義存檔名稱
                lat_s = str(lat_round_lst[j]).replace(".", "-")
                lon_s = str(lon_round_lst[j]).replace(".", "-")
                file_name = f"{save_dir}/{lat_s}_{lon_s}.parquet"

                # 存成 Parquet 較節省空間(直接存到GCS上)，印出進度
                try:
                    print(f"正在處理: {(j+1)}/{len(data)} file")

                    # 最穩定的 (io.BytesIO + engine='pyarrow')可以避開 pandas內部讀寫GCS可能產生的版本衝突
                    buffer = io.BytesIO()
                    df_a_loc_hourly.to_parquet(
                        buffer, index=False, engine='pyarrow')
                    buffer.seek(0)
                    gcs_hook.upload(
                        bucket_name=bucket_name,
                        object_name=file_name,
                        data=buffer.getvalue(),  # parquet資料透過data傳入，預設會覆蓋同名檔案
                        mime_type='application/octet-stream')
                except Exception as e:
                    print(f"第{j+1}/{len(data)} file寫入失敗! {e}")
                else:
                    print(f"已成功寫入第{j+1}/{len(data)} file: {file_name}!")

    else:
        # 印出前100個字看發生什麼事，印出進度
        print(f"API回傳內容: {str(data)[:100]}")

    # 7. sleep緩解server負擔
    time.sleep(random.uniform(5, 10))
    return str(bucket_name)
