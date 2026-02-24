from sqlalchemy import text
import hashlib
import io
import sys
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from airflow.sdk import task
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到 ./utils下的模組或套件
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# 2. 在sys.path之後才進行import
from utils.create_engine_conn_tomysql import get_engine_sqlalchemy, get_conn_pymysql, writer
from utils.create_weather_hist_table import create_weather_hist_table
from utils.get_table_from_mysql_gcp import get_table_from_sqlserver


def t_dataclr_weather_hist(df_weather_raw: pd.DataFrame,
                           df_view_loc_acc: pd.DataFrame) -> pd.DataFrame:
    """
        Transform: 清理出跟車禍事故日期時間相近的天氣觀測資料、淘汰不相關時間點的冗餘天氣觀測資料。
                最後生成hash確保 觀測日期時間、經度、緯度 的業務語意唯一性。

        :param df_weather_raw: 從OpenMeteo API下載下來的原始整年度天氣觀測資料，為dataframe
        :type df_weather_raw: pd.DataFrame
        :param df_view_loc_acc: 描述每個車禍地點經緯度進位至小數點後二位的結果之dataframe
        :type df_view_loc_acc: pd.DataFrame
        :return: 車禍事故日期時間相近的天氣觀測資料之dataframe，若沒有時間相近的天氣資料，
                 則回傳empty dataframe
        :rtype: DataFrame
    """

    # 1. 清理df_weather_raw
    # 清理日期，統一成YYYY-mm-dd HH:MM:SS的格式並先維持字串
    df_weather_raw["datetime_ISO8601"] = df_weather_raw["datetime_ISO8601"].astype(
        str).str.replace("T", " ").str.replace(":00", ":00:00")

    # 清理visibility_m，轉為float64
    df_weather_raw["visibility_m"] = df_weather_raw["visibility_m"].astype(
        "float64")

    # 2. 濾掉跟車禍日與車禍地點不相干的資料列 (此舉在幫助將1億筆壓成150萬筆等級的資料列)
    df_weather_mrg = df_view_loc_acc.merge(df_weather_raw, how="inner",
                                           left_on=[
                                               "approx_accident_datetime", "longitude_round", "latitude_round"],
                                           right_on=[
                                               "datetime_ISO8601", "longitude_round", "latitude_round"],
                                           suffixes=["_a", "_w"])

    df_weather_mrg = df_weather_mrg.loc[:, ["datetime_ISO8601", "temperature_2m_degree",
                                            "apparent_temperature_degree",
                                            "rain_mm", "precipitation_mm", "visibility_m",
                                            "weather_code", "longitude_round", "latitude_round"]]

    # 3. 正式置換成寫入資料表所需的欄位名稱
    df_weather_mrg.columns = ["observation_datetime", "temperature_degree",
                              "apparent_temperature_degree", "rain_within_hour_mm",
                              "precipitation_mm", "visibility_m", "weather_code",
                              "longitude_round", "latitude_round"]

    # 4. 生成UK，由於構成業務邏輯唯一的組成有observation_datetime、long_round、lat_round，後二者是浮點數，
    # 浮點數作為SQL中的UK時，需擔心讀取結果跟pandas的讀取結果不同，而發生業務邏輯在寫入前後不一致的問題，所以這裡採用hash value，
    # 從ETL的T層，控管在寫入MySQL前就生好重複性低的hash value當作UK。
    uk = (df_weather_mrg["observation_datetime"].astype(str) + "|" +
          df_weather_mrg["longitude_round"].astype(str) + "|" +
          df_weather_mrg["latitude_round"].astype(str)
          )
    df_weather_mrg["hash_value"] = uk.apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()[:32])

    # 5. 觀望一下hash 後是否有重複的
    dup_cnt = df_weather_mrg["hash_value"].duplicated().sum()
    print("T層內部重複筆數:", dup_cnt)

    # 有可能一筆天氣觀測資料對應到多個事故地點，但此時我們的表只管寫入某地點的天氣觀測資料，不在意每個事故對應的天氣(那是後面資料分析做join的工作)
    # 6. 因此建議要drop_duplicates()
    df_weather_mrg = df_weather_mrg.drop_duplicates(subset=["hash_value"])

    # 7. 填補空值
    df_weather_final = df_weather_mrg.where(pd.notnull(df_weather_mrg), None)
    df_weather_final = df_weather_final.replace({np.nan: None})

    # 8. 淘汰不再使用於下游task的變數，以釋放記憶體
    del df_weather_raw, df_weather_mrg
    import gc
    gc.collect()
    return df_weather_final


@task
def l_summary_report(target_year: int, upstream) -> str:
    """
        Load: 載入與確認 - 檢查最終data warehouse (GCS)上面存放的parquet檔案數量

        :param target_year: 要查詢哪一年份的天氣觀測資料儲存區
        :type target_year: int
        :param upstream: 此任務依賴於哪個上游任務之return value(通常是e_crawler_weatherapi() func.)
        :return:  GCS bucke名稱字串
        :rtype: str
    """

    # 1. 找出該年份的所有Parquet之檔案路徑
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')  # 初始化
    bucket_name = 'tjr104-01_weather'
    save_dir = f"weather_cache/{target_year}"
    all_files = gcs_hook.list(bucket_name=bucket_name,
                              prefix=save_dir)
    all_files = [f for f in all_files if f.endswith(".parquet")]

    # 2. 引出總結報告
    print(f"============== Summary: {target_year} ==============")
    print(f"Year: {target_year}")
    print(f"Storage path: {bucket_name}/{save_dir}")
    print(f"Total Parquet files collected: {len(all_files)}")
    print("==============================================")
    return str(bucket_name)


@task(retries=2, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(hours=4))
def l_transform_and_load_to_mysql(target_year: int, database: str,
                                  upstream) -> None:
    """
        Load: 打開GCS中存好的parquet檔，並清理(T)後直接寫入MySQL。採批次處理，以for-loop按批執行T+L，
        以減緩MySQL連線次數與RAM一次讀取大量資料的壓力。

        :param target_year: 要清理與寫入資料庫的資料所屬年份
        :type target_year: int
        :param database: 打算寫入的資料庫名稱
        :type database: str
        :param upstream: 此任務依賴於哪個上游任務之return value(通常是l_summary_report() func.)
        :return: 
        :rtype: None
    """

    # 1. 讀取車禍view表
    dql_text = f"""SELECT approx_accident_datetime, longitude_round, latitude_round
	                    FROM v_acc_approx_loc_{target_year};"""
    df_view_loc_acc = get_table_from_sqlserver(database, dql_text)

    # 2. 保險起見再對齊一次資料型態
    df_view_loc_acc["approx_accident_datetime"] = df_view_loc_acc["approx_accident_datetime"].astype(
        str)
    df_view_loc_acc["longitude_round"] = df_view_loc_acc["longitude_round"].astype(
        "float64").round(2)
    df_view_loc_acc["latitude_round"] = df_view_loc_acc["latitude_round"].astype(
        "float64").round(2)

    # 3. 準備與MySQL server的連線，用作為底層驅動的pymysql建立conn
    conn = get_conn_pymysql(database)

    # 4. 用作為底層驅動的pymysql建立conn＆cursor
    cursor = conn.cursor()

    # 5. 找出該年份的所有Parquet之檔案路徑
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')  # 初始化
    bucket_name = 'tjr104-01_weather'
    save_dir = f"weather_cache/{target_year}"
    all_files = gcs_hook.list(bucket_name=bucket_name,
                              prefix=save_dir)
    all_files = [f for f in all_files if f.endswith(".parquet")]

    if not all_files:
        print(f"No weather data found for {target_year}")
        return None

    # 6. 先確保表已存在
    this_year = datetime.now().year
    table_name = "weather_hourly_history_final" if target_year < this_year else "weather_hourly_now"
    create_weather_hist_table(database, table_name)

    # 7. 逐一檔案處理，避免記憶體爆炸
    try:
        total_rows = 0
        batch_size = 50
        for i in range(0, len(all_files), batch_size):
            batch_files = all_files[i: i + batch_size]

            # 7-1. 讀取50個檔案後合併在一個大dataframe
            df_list = []  # 將讀取的df收在一個清單
            print(f"Downloading Batch No {i // batch_size + 1} from GCS.....")
            for f in batch_files:
                file_data = gcs_hook.download(
                    bucket_name=bucket_name, object_name=f)
                df_w_chunk = pd.read_parquet(io.BytesIO(file_data))
                df_list.append(df_w_chunk)

            # 7-2. 清單製備好後一口氣合併
            print(f"Downloading finished. Starting to clean data.....")
            df_weather_raw = pd.concat(df_list, ignore_index=True)

            if df_weather_raw.empty:
                continue

            # 7-3.合併後開始清洗
            df_transformed = t_dataclr_weather_hist(
                df_weather_raw, df_view_loc_acc)
            df_transformed["created_by"] = writer

            # 7-4. 準備寫入資料表的語句
            # 直接用pymysql批量插入(完全避開 pandas 3.0+與airflow3.0/sqlalchemy 2.0的不相容問題)
            columns = ', '.join(df_transformed.columns)
            placeholders = ', '.join(['%s'] * len(df_transformed.columns))
            columns_list = df_transformed.columns.tolist()

            # 7-5. 動態生成UPDATE部分：排除掉 hash_value，剩下的都要更新
            # 效果如:temperature_degree=VALUES(temperature_degree), rain_mm=VALUES(rain_mm)...
            update_part = ', '.join(
                [f"{col}=VALUES({col})" for col in columns_list if col != 'hash_value'])

            insert_sql = f"""
                            INSERT INTO {table_name} ({columns})
                            VALUES ({placeholders})
                            ON DUPLICATE KEY UPDATE {update_part}
                        """

            # 7-6. 寫入資料表
            print(f"Inserting into MySQL table....")
            cursor.executemany(insert_sql, df_transformed.values.tolist())
            total_rows += len(df_transformed)
            print(
                f"Batch {i // batch_size + 1} finished: Inserted {len(df_transformed)} rows, Total so far: {total_rows}")

            # # 另外一種寫法，但或許得將pandas降到2.0.3，兼容性較好，不容易報錯(e.g. pandas only supports SQLAlchemy connectabl)
            # engine = get_engine_sqlalchemy(database)
            # df_transformed.to_sql(name=table_name,
            #                       con=engine,
            #                       if_exists="append",
            #                       index=False,
            #                       method="multi",
            #                       chunksize=1000  # 每批1000筆，降低網路超時機率
            #                       )

            # 8. 手動釋放該檔案佔用的記憶體
            del df_weather_raw, df_transformed, df_list
            import gc
            gc.collect()
    except Exception as e:
        print(f"Error on Batch No {i // batch_size + 1}, Error msg: {e}")
    else:
        print(f"Successfully loaded {target_year} data to GCP MySQL.")
    finally:
        # 9. 關閉cursor與conn
        cursor.close()
        conn.close()
        return f"{target_year}_load_done"


@task
def l_load_to_mysql(target_year: int, database: str,
                    df_uniq_loc_acc: pd.DataFrame,
                    upstream) -> None:
    """已棄用"""
    # 1. 遍歷該年份的所有 Parquet之檔案路徑
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')  # 初始化
    bucket_name = 'tjr104-01_weather'
    save_dir = f"weather_cache/{target_year}"
    all_files = gcs_hook.list(bucket_name=bucket_name,
                              prefix=save_dir)
    all_files = [f for f in all_files if f.endswith(".parquet")]

    if not all_files:
        print(f"No weather data found for {target_year}")
        return None

    # 2. 再次讀取車禍資料表並取得去重後的經緯度
    df_acc = df_uniq_loc_acc.copy()

    # 3. 準備與GCP VM上的MySQL server的連線
    engine = get_engine_sqlalchemy(database)

    # 4. 設定每 100 個檔案處理一次
    batch_size = 100
    for i in range(0, len(all_files), batch_size):
        batch_files = all_files[i: i + batch_size]

        # 讀取這一小批
        df_list = []
        for file_path in batch_files:
            file_data = gcs_hook.download(
                bucket_name=bucket_name, object_name=file_path)
            df_list.append(pd.read_parquet(io.BytesIO(file_data)))

        df_weather = pd.concat(df_list, ignore_index=True)

        # 4-1. 合併資料 (根據 經度、緯度進行 Join)
        df_weather_final = df_weather.merge(df_acc, how="left",
                                            left_on=["latitude_round",
                                                     "longitude_round"],
                                            right_on=[
                                                "lat_round", "lon_round"],
                                            suffixes=["_w", "_a"])

        # df_weather_final = df_weather_final.drop(["lat_round", "lon_round"])
        data_list = df_weather_final.where(pd.notnull(
            df_weather_final), None).to_dict(orient='records')
        # data_list = df_weather_final.to_dict(orient='records')

        # 取得欄位名稱
        cols = df_weather_final.columns.tolist()
        table_name = f"accident_weather_{target_year}"

        del df_weather_final, df_weather
        import gc
        gc.collect()
        with engine.begin() as conn:
            if data_list and i == 0:
                # 4-1. 強制刪除舊表
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

                # 4-2. 手動建立表結構 (簡單地將所有欄位設為 TEXT，或根據需要調整)
                # 根據欄位名生成 CREATE TABLE 語句
                create_stmt = f"CREATE TABLE {table_name} (" + ", ".join(
                    [f"`{c}` TEXT" for c in cols]) + ")"
                conn.execute(text(create_stmt))

            # 4-3. 正式寫入資料
            # 使用 SQLAlchemy 的 bind parameter 語法 (MySQL 使用 :column_name)
            insert_stmt = text(
                f"INSERT INTO {table_name} ({', '.join([f'`{c}`' for c in cols])}) "
                f"VALUES ({', '.join([':' + c for c in cols])})"
            )

            # 4-4. 執行批量插入
            conn.execute(insert_stmt, data_list)

            # 4-5. 清除記憶體
            del data_list, df_list
            import gc
            gc.collect()
            print("清除記憶體完成!")
    print(f"Successfully loaded {target_year} data to GCP MySQL.")
    return None
