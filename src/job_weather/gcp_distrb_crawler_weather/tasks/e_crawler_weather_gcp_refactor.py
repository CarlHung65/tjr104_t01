import pandas as pd
from datetime import datetime, timedelta
import time
import random
from pathlib import Path
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
from utils.request_weather_api import request_weather_api


@task(retries=3, retry_delay=timedelta(hours=1), execution_timeout=timedelta(hours=48))
def e_crawler_weather_refactor(df_uniquee_loc_acc: pd.DataFrame,
                               target_year: int, loc_num_per_request=10) -> str:
    # loc_num_per_request為defalut設定批次API請求，一次請求取10個地點

    # 預備存parquet路徑(直接存到GCS上)
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')  # 初始化
    bucket_name = 'tjr104-01_weather'
    save_dir = f"weather_cache/{target_year}"

    # 提取一個年度要搜尋的地點
    df_unique_loc_y = df_uniquee_loc_acc.copy()

    # 給定每日請求限額
    daily_request_quota = 9000  # 官網要求non-commerical 1萬/天

    # 利用for-loop取出每一次請求下的請求內容，內容包含：經緯度。
    for i in range(0, len(df_unique_loc_y), loc_num_per_request):
        df_a_batch = df_unique_loc_y.iloc[i: i+loc_num_per_request]

        # 先盤點df_a_batch所請求的經緯度是否已經爬過且存下parquet了
        needed_loc = []
        for _, row in df_a_batch.iterrows():
            lat_s = str(row['lat_round']).replace(".", "-")
            lon_s = str(row['lon_round']).replace(".", "-")
            file_name = f"{save_dir}/{lat_s}_{lon_s}.parquet"

            if not gcs_hook.exists(bucket_name, file_name):
                needed_loc.append(row)  # 每一row都是一個series

        # 如果這批次所有地點都爬過了，直接跳過這個batch
        if not needed_loc:
            print(f"第{i}到{i+loc_num_per_request-1}個資料列所示地點的對應parquet皆已存在，跳過。")
            continue
        else:
            # 精煉出真的需要爬取的經緯度清單，長度必須一致
            # r是series,用col_name取值
            lat_round_lst = [r['lat_round'] for r in needed_loc]
            lon_round_lst = [r['lon_round'] for r in needed_loc]

            # # debug區：測試一下組合的結果，穩定後可以註解掉
            print("緯度長度: ", len(lat_round_lst))
            print("經度長度:", len(lon_round_lst))

            # 把真正要請求的經緯度從list轉成組合字串
            lats_str = ",".join([str(r) for r in lat_round_lst])
            lons_str = ",".join([str(r) for r in lon_round_lst])

            # 準備要拋給API的param，一次針對10個經緯地點，抓一年份數據，中間會逐點存檔，但整體而言這樣算一次請求
            start = f"{target_year}-01-01"
            end = f"{target_year}-12-31"
            vars = ["temperature_2m", "apparent_temperature", "rain",
                    "showers", "precipitation", "visibility", "weather_code"]
            col_name = ['datetime_ISO8601', 'temperature_2m_degree',
                        'apparent_temperature_degree', 'rain_mm',
                        'showers_mm', 'precipitation_mm',
                        'visibility_m', 'weather_code',
                        'latitude_round', 'longitude_round']

            # 讀取今日已requests次數，其key名從var_key宣告之
            today_str = datetime.now().strftime('%Y-%m-%d')
            # 使用今天的日期字串來命名var_key
            var_key = f"weather_api_count_{target_year}_{today_str}"

            # 取出var_key後，存入此迴圈的暫時變數curr_request
            curr_request = int(Variable.get(var_key, default_var=0))

        if curr_request >= daily_request_quota:
            print("已達到今日API 10,000 次限制，剩餘資料將由下次任務處理。")
            break
        else:
            # 調用utils裡的API function，找一個地點一年度的資料
            print("Not reaching daily_quota_limit, calling for API......")
            data = request_weather_api(
                lats_str, lons_str, start, end, vars)  # return a list of "dicts_of list"

        if data:
            # 先記錄本次算是今日第幾次請求，並存入var_key
            curr_request += 1
            print(f"已calling{curr_request}次")
            Variable.set(var_key, str(curr_request))

            # 開始清洗
            data = data if isinstance(data, list) else [data]
            for j in range(0, len(data)):  # 長度等同於len(lat_round_lst)
                if "hourly" in data[j]:  # a dict of one certain location
                    # data[i]["hourly"] is a dict
                    hourly_df_a_loc = pd.DataFrame(data[j]['hourly'])
                    hourly_df_a_loc['latitude_round'] = lat_round_lst[j]
                    hourly_df_a_loc['longitude_round'] = lon_round_lst[j]

                    # 置換成想要的欄位名稱
                    hourly_df_a_loc.columns = col_name

                    # 定義存檔名稱
                    lat_s = str(lat_round_lst[j]).replace(".", "-")
                    lon_s = str(lon_round_lst[j]).replace(".", "-")
                    file_name = save_dir/f"{lat_s}_{lon_s}.parquet"

                    # 存成 Parquet 較節省空間(直接存到GCS上)，印出進度
                    # 最穩定的 (io.BytesIO + engine='pyarrow')可以避開 pandas內部讀寫GCS可能產生的版本衝突
                    buffer = io.BytesIO()
                    hourly_df_a_loc.to_parquet(
                        buffer, index=False, engine='pyarrow')
                    buffer.seek(0)

                    gcs_hook.upload(
                        bucket_name=bucket_name,
                        object_name=file_name,
                        data=buffer.getvalue(),  # parquet資料透過data傳入
                        mime_type='application/octet-stream'
                    )
                    print(f"已成功寫入本批第{j+1}個file: {file_name}!")

        else:
            # 印出前100個字看發生什麼事，印出進度
            print(f"API回傳內容: {str(data)[:100]}")

        # 總結迴圈這一輪
        print(f"已完成: {i+loc_num_per_request}/{len(df_unique_loc_y)}")

        # 不管是否成功取得response，都睡60-180秒之間。
        time.sleep(random.uniform(60, 180))
    return str(bucket_name)
