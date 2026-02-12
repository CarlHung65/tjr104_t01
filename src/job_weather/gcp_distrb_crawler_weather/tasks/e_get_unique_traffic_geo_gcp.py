import sys
import os
import pandas as pd
from sqlalchemy import text
from pathlib import Path
from airflow.sdk import task

# 1. 先確保路徑進去了
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# 2. 在 sys.path 之後才進行 import
from utils.get_table_from_mysql_gcp import get_table_from_sqlserver


@task
def e_get_unique_traffic_geo(target_year: int) -> pd.DataFrame:
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
    df_acc["lat_round"] = df_acc["latitude"].astype("Float64").round(2)
    df_acc["lon_round"] = df_acc["longitude"].astype("Float64").round(2)

    df_uniq_loc_acc = df_acc.drop_duplicates(['lat_round', 'lon_round'])
    print(
        f"Year {target_year}: Found {len(df_uniq_loc_acc)} unique locations.")

    # a df with [accident_year, longitude, latitude, lat_round, lon_round]
    return df_uniq_loc_acc
