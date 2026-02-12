from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus
from pathlib import Path
import io
import pandas as pd
from airflow.sdk import task
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook


@task
def l_load_to_mysql(target_year: int, database: str,
                    df_uniq_loc_acc: pd.DataFrame) -> None:
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
    load_dotenv(Path().cwd())
    username = os.getenv("gcp_mysql_vm_user")
    password = os.getenv("gcp_mysql_vm_passwd")
    host = os.getenv("gcp_mysql_vm_host")
    port = os.getenv("gcp_mysql_vm_port")
    charset = os.getenv("charset", "utf8mb4")
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
        # # 使用 chunksize 避免記憶體爆炸
        # current_if_exists = 'replace' if i == 0 else 'append'
        # df_weather_final.to_sql(
        #     name=f"accident_weather_{target_year}",
        #     con=engine,
        #     if_exists='current_if_exists',
        #     index=False,
        #     chunksize=5000
        # )
    print(f"Successfully loaded {target_year} data to GCP MySQL.")
    return None
