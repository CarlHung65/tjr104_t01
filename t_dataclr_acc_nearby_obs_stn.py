import pandas as pd
import geopandas as gpd  # poetry add geopandas。GeoPandas輔助判斷什麼叫「附近」、「重疊」。
from sqlalchemy import create_engine, types, text
from pathlib import Path
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from datetime import datetime


def find_nearby_Obs_stn_in_buffer_area(accident_df: pd.DataFrame,
                                       station_df: pd.DataFrame) -> pd.DataFrame:
    # Step2-1: 將 Pandas的DataFrame 轉換為 GeoPandas的GeoDataFrame
    gdf_Accident = gpd.GeoDataFrame(accident_df,
                                    # geometry 把經緯度字串轉成pandas看得懂的"點(Point)"，並放在geometry這新的一欄。
                                    geometry=gpd.points_from_xy(
                                        accident_df["longitude"], accident_df["latitude"]),
                                    crs="EPSG:4326",)

    gdf_Obs_stn = gpd.GeoDataFrame(station_df,
                                   geometry=gpd.points_from_xy(
                                       station_df["station_longitude_WGS84"], station_df["station_latitude_WGS84"]),
                                   crs="EPSG:4326")

    # Step2-2: 將geometry那欄的座標系轉換為"EPSG 3826公尺座標系"，代表事故地點在公尺座標系下的位置
    gdf_Accident_3826 = gdf_Accident.to_crs(epsg=3826)
    gdf_Obs_stn_3826 = gdf_Obs_stn.to_crs(epsg=3826)

    # Step2-3: 為事故地點建立 半徑10000 公尺的緩衝區 (Buffer)
    gdf_Accident_3826['buffer'] = gdf_Accident_3826.geometry.buffer(
        distance=10000)  # 會多加一欄buffer，資料型別也是Geometry

    # Step2-4: 空間連接(Spatial Join)、找區域交疊
    # 代表改用buffer的值當作事故地點的定位，而不是剛剛產生的geometry，即:把 點(Point) 換成 面(Polygon)
    gdf_Accident_3826 = gdf_Accident_3826.set_geometry("buffer")

    # Step2-5: 尋找事故地點附近10000公尺的緩衝區內是否出現觀測站，並全部列出。
    nearby_Obs_stn = gpd.sjoin(
        gdf_Accident_3826, gdf_Obs_stn_3826, how="left", predicate="intersects")

    # Step2-6: 把原本算好的觀測站point(at epsg 3826)合併
    nearby_Obs_stn = nearby_Obs_stn.merge(gdf_Obs_stn_3826, how="left",
                                          on="station_record_id", suffixes=["_left", "_right"])

    # 合併後，為每一筆觀測站與事故地點的point做距離計算，新增到distance欄位
    nearby_Obs_stn["distance"] = nearby_Obs_stn["geometry_left"].distance(
        nearby_Obs_stn["geometry_right"])

    # Step2-7: 踢除不需呈現的欄位
    nearby_Obs_stn = nearby_Obs_stn[["accident_id", "accident_datetime",
                                    "station_record_id", "station_id_left",
                                     "state_valid_from_left", "distance"]]

    # Step2-8: 剔除State_valid_from_left晚於accident_datetime的資料表，因為此情境會無法順利取得accident_datetime天氣
    nearby_Obs_stn = nearby_Obs_stn[nearby_Obs_stn["state_valid_from_left"] <
                                    nearby_Obs_stn["accident_datetime"]]

    # Step2-9: 為每個事故地點排出2個最近的觀測站，並且有標記名次(近者排名1、遠則排名2)

    nearby_two_stn = nearby_Obs_stn.sort_values(
        by=["accident_id", "distance"], axis=0, ascending=[True, True], na_position="first")
    nearby_two_stn["rank_of_distance"] = nearby_two_stn.groupby(
        "accident_id").cumcount()+1
    nearby_two_stn = nearby_two_stn[nearby_two_stn["rank_of_distance"] <= 2]

    # 修改欄位命名
    nearby_two_stn.columns = [
        "accident_id", "accident_datetime",
        "station_record_id", "station_id",
        "state_valid_from", "distance", "rank_of_distance"]

    # 確保一下data type沒跑掉，尤其是日期時間。
    nearby_two_stn["accident_datetime"] = pd.to_datetime(
        nearby_two_stn["accident_datetime"], "coerce", format="mixed").dt.strftime("%Y-%m-%d %H:%M:%S")
    nearby_two_stn["state_valid_from"] = pd.to_datetime(
        nearby_two_stn["state_valid_from"], "coerce", format="mixed").dt.strftime("%Y-%m-%d")
    return nearby_two_stn


def get_table_from_sqlserver(database: str, dql_text) -> pd.DataFrame:
    """Read the desingated table in MySQL server. """

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
    conn = create_engine(
        f"mysql+pymysql://{username}:{password}@{host}:{port}/{db_name}?charset={charset}",
        echo=False,  # 使用預設值，不印SQL日誌，保持乾淨輸出，生產環境適用
        pool_pre_ping=True,  # 檢查連線有效性
        pool_recycle=300,    # 每5分鐘自動重整連線，可再調整
        connect_args={'connect_timeout': 60}).connect()

    df = pd.read_sql(dql_text, conn)
    return df


def t_dataclr_acc_nearby_obs_stn():
    try:
        # Step 1: 讀取事故資訊表與Obs_stations
        df_acc = get_table_from_sqlserver(
            "test_accident", text("""SELECT * FROM accident_sq1_main"""))

        df_Obs_stn = get_table_from_sqlserver("test_weather", text("""SELECT `station_record_id`, `station_id`, 
                                                                    `station_longitude_WGS84`, 
                                                                    `station_latitude_WGS84`, `state_valid_from`
                                                                    FROM obs_stations
                                                                        WHERE station_working_state = 'Running';
                                                                    """))

        # Step 2: 轉換座標，找尋周圍的氣象觀測站
        nearby_Obs_stn = find_nearby_Obs_stn_in_buffer_area(df_acc, df_Obs_stn)
    except Exception as e:
        print(f"Error: {e}")
    return nearby_Obs_stn


nearby_Obs_stn = t_dataclr_acc_nearby_obs_stn()
print(f"Simple description for nearby observation stations before load to MySQL server: "
      f"{nearby_Obs_stn.info()}")

# if __name__ == "__main__":
#     # 以下是本地端測試區：存成csv備份
#     curr_dir = Path().resolve()
#     save_path = curr_dir/"processed_csv"/"accident_nearby_obs_stn"
#     save_path.mkdir(parents=True, exist_ok=True)
#     filename = save_path/f"accident_nearby_obs_stn_{datetime.now().date()}.csv"
#     nearby_Obs_stn.to_csv(filename, encoding="utf-8-sig", index=False)
