
import os  # 負責：跟你的作業系統溝通，建立資料夾、串接路徑、檢查檔案
from sqlalchemy import types #types用在python裡定義mysql的資料型別
from dotenv import load_dotenv #用來讀取.env
# --- 變數名稱定義 (全域統一，方便管理) ---
PAGE_URL = "https://data.gov.tw/dataset/172969"  # 資料集介紹頁
# SAVE_OLD_DATA_DIR = r"C:\TJR104_Project\CSV_Old_Data"      # 存檔目錄
# SAVE_NEW_DATA_DIR = r"C:\TJR104_Project\CSV_New_Data"
SAVE_OLD_DATA_DIR = os.path.join(os.getcwd(), "CSV_Old_Data")
SAVE_NEW_DATA_DIR = os.path.join(os.getcwd(), "CSV_New_Data")
SAVE_NEW_DATA_PATH = os.path.join(SAVE_NEW_DATA_DIR)  # 最終壓縮檔路徑
# 資料庫連線字串：帳號:密碼@伺服器位址:埠號/資料庫名稱
load_dotenv()
user = os.getenv("DB_USER", "root")
password = os.getenv("DB_PASS")
host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "3307")
dbname = os.getenv("DB_NAME", "test_accident")

DB_URL = "mysql+pymysql://root:nadrew8425@localhost:3306/TJR104_Project"
GCP_DB_URL = f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}"
# 模擬真人瀏覽器的標頭，避免被伺服器偵測為機器人而拒絕連線
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
    'Referer': 'https://data.gov.tw/'
}
SEQ_PAGE_URL = ["https://data.gov.tw/dataset/158865","https://data.gov.tw/dataset/161199",
                "https://data.gov.tw/dataset/167905","https://data.gov.tw/dataset/172969"]
RECENT_PAGE_A1_URL = "https://data.gov.tw/dataset/12818"
RECENT_PAGE_A2_URL = "https://data.gov.tw/dataset/13139"

COL_MAP = {
        "發生年度": "accident_year", "發生月份": "accident_month",
        "發生日期": "accident_date", "發生時間": "accident_time",
        "事故類別名稱": "accident_category", "處理單位名稱警局層": "police_department",
        "發生地點": "accident_location", "天候名稱": "weather_condition",
        "光線名稱": "light_condition", "道路類別-第1當事者-名稱": "road_type_primary_party",
        "速限-第1當事者": "speed_limit_primary_party", "道路型態大類別名稱": "road_form_major",
        "道路型態子類別名稱": "road_form_minor", "事故位置大類別名稱": "accident_position_major",
        "事故位置子類別名稱": "accident_position_minor", "路面狀況-路面鋪裝名稱": "road_surface_pavement",
        "路面狀況-路面狀態名稱": "road_surface_condition", "路面狀況-路面缺陷名稱": "road_surface_defect",
        "道路障礙-障礙物名稱": "road_obstacle", "道路障礙-視距品質名稱": "sight_distance_quality",
        "道路障礙-視距名稱": "sight_distance", "號誌-號誌種類名稱": "traffic_signal_type",
        "號誌-號誌動作名稱": "traffic_signal_action", "車道劃分設施-分向設施大類別名稱": "lane_divider_direction_major",
        "車道劃分設施-分向設施子類別名稱": "lane_divider_direction_minor", "車道劃分設施-分道設施-快車道或一般車道間名稱": "lane_divider_main_general",
        "車道劃分設施-分道設施-快慢車道間名稱": "lane_divider_fast_slow", "車道劃分設施-分道設施-路面邊線名稱": "lane_edge_marking",
        "事故類型及型態大類別名稱": "accident_type_major", "事故類型及型態子類別名稱": "accident_type_minor",
        "肇因研判大類別名稱-主要": "cause_analysis_major_primary", "肇因研判子類別名稱-主要": "cause_analysis_minor_primary",
        "死亡受傷人數": "casualties_count", "當事者順位": "party_sequence",
        "當事者區分-類別-大類別名稱-車種": "vehicle_type_major", "當事者區分-類別-子類別名稱-車種": "vehicle_type_minor",
        "當事者屬-性-別名稱": "gender", "當事者事故發生時年齡": "age",
        "保護裝備名稱": "protective_equipment", "行動電話或電腦或其他相類功能裝置名稱": "mobile_device_usage",
        "當事者行動狀態大類別名稱": "party_action_major", "當事者行動狀態子類別名稱": "party_action_minor",
        "車輛撞擊部位大類別名稱-最初": "impact_point_major_initial", "車輛撞擊部位子類別名稱-最初": "impact_point_minor_initial",
        "車輛撞擊部位大類別名稱-其他": "impact_point_major_other", "車輛撞擊部位子類別名稱-其他": "impact_point_minor_other",
        "肇因研判大類別名稱-個別": "cause_analysis_major_individual", "肇因研判子類別名稱-個別": "cause_analysis_minor_individual",
        "肇事逃逸類別名稱-是否肇逃": "hit_and_run", "經度": "longitude", "緯度": "latitude"
    }

MAIN_COL = ['accident_id','accident_category', 'accident_datetime', 'accident_weekday',
           'death_count','injury_count','weather_condition','party_sequence','longitude','latitude']
    
ENVIRONMENT_COL =['accident_id','light_condition','road_type_primary_party','speed_limit_primary_party','road_form_major',
                  'road_form_minor','accident_position_major','accident_position_minor',
                  'road_surface_pavement','road_surface_condition','road_surface_defect',
                  'road_obstacle','sight_distance_quality','sight_distance','traffic_signal_type',
                  'traffic_signal_action','lane_divider_direction_major','lane_divider_direction_minor',
                  'lane_divider_main_general','lane_divider_fast_slow','lane_edge_marking']

HUMAN_BEAHAVIOR_COL =['accident_id','gender',"age","protective_equipment","mobile_device_usage"
                       ,"party_action_major","party_action_minor"]

EVENT_PROCESS_PARTICIPATE_OBJECT_COL = ['accident_id','accident_type_major','accident_type_minor',
                                    'cause_analysis_major_primary','cause_analysis_minor_primary',
                                    'cause_analysis_major_individual','cause_analysis_minor_individual',
                                    'vehicle_type_major','vehicle_type_minor']
EVENT_RESULT_COL = ['accident_id','accident_category','impact_point_major_initial','impact_point_minor_initial',
                'impact_point_major_other','impact_point_minor_other','hit_and_run']


MAIN_TABLE_DICT = {
        'accident_id': types.VARCHAR(16),
        'accident_category':types.VARCHAR(2),
        'accident_datetime': types.DateTime(),
        'accident_weekday':types.Integer(),
        'death_count':types.INTEGER,
        'injury_count':types.INTEGER,
        'weather_condition':types.VARCHAR(10),
        'party_sequence':types.INTEGER,
        'longitude':types.DECIMAL(10,6),
        'latitude':types.DECIMAL(10,6)
}

ENVIRONMENT_TABLE_DICT={'accident_id':types.VARCHAR(16),'light_condition':types.VARCHAR(20),
                        'road_type_primary_party':types.VARCHAR(10),'speed_limit_primary_party':types.SMALLINT,
                        'road_form_major':types.VARCHAR(10),'road_form_minor':types.VARCHAR(10),
                  'accident_position_major':types.VARCHAR(20),'accident_position_minor':types.VARCHAR(20),
                  'road_surface_pavement':types.VARCHAR(10),'road_surface_condition':types.VARCHAR(10),
                  'road_surface_defect':types.VARCHAR(10),'road_obstacle':types.VARCHAR(10),
                  'sight_distance_quality':types.VARCHAR(10),'sight_distance':types.VARCHAR(10),
                  'traffic_signal_type':types.VARCHAR(50),'traffic_signal_action':types.VARCHAR(10),
                  'lane_divider_direction_major':types.VARCHAR(20),'lane_divider_direction_minor':types.VARCHAR(20),
                  'lane_divider_main_general':types.VARCHAR(20),'lane_divider_fast_slow':types.VARCHAR(20),
                  'lane_edge_marking':types.BOOLEAN}

HUMAN_BEAHAVIOR_DICT = {'accident_id': types.VARCHAR(16),'gender':types.VARCHAR(20),
                       'age':types.SMALLINT,'protective_equipment':types.VARCHAR(50),
                       'mobile_device_usage':types.VARCHAR(20),'party_action_major':types.VARCHAR(20),
                       'party_action_minor':types.VARCHAR(20)}

EVENT_PROCESS_PARTICIPATE_OBJECT_DICT= {'accident_id': types.VARCHAR(16),'accident_type_major':types.VARCHAR(20),
                                        'accident_type_minor':types.VARCHAR(20),'cause_analysis_major_primary':types.VARCHAR(20),
                                        'cause_analysis_minor_primary':types.VARCHAR(120),'cause_analysis_major_individual':types.VARCHAR(20),
                                        'cause_analysis_minor_individual':types.VARCHAR(100),'vehicle_type_major':types.VARCHAR(20),
                                        'vehicle_type_minor':types.VARCHAR(20)}

EVENT_RESULT_DICT ={'accident_id': types.VARCHAR(16),'accident_category':types.VARCHAR(2),'impact_point_major_initial':types.VARCHAR(20),
                    'impact_point_minor_initial':types.VARCHAR(20),'impact_point_major_other':types.VARCHAR(20),
                    'impact_point_minor_other':types.VARCHAR(20),'hit_and_run':types.VARCHAR(2)}

# DETAIL_TABLE_DICT={
#     'accident_id': types.VARCHAR(16),'accident_category':types.VARCHAR(2), 'police_department':types.VARCHAR(2), 
#         'accident_location':types.VARCHAR(100), 'light_condition':types.VARCHAR(20),
#         'road_type_primary_party':types.VARCHAR(10),'speed_limit_primary_party':types.SMALLINT,
#         'road_form_major':types.VARCHAR(10),'road_form_minor':types.VARCHAR(10),
#         'accident_position_major':types.VARCHAR(20),'accident_position_minor':types.VARCHAR(20),
#         'road_surface_pavement':types.VARCHAR(10),'road_surface_condition':types.VARCHAR(10),
#         'road_surface_defect':types.VARCHAR(10),'road_obstacle':types.VARCHAR(10),
#         'sight_distance_quality':types.VARCHAR(10),'sight_distance':types.VARCHAR(10),
#         'traffic_signal_type':types.VARCHAR(10),'traffic_signal_action':types.VARCHAR(10),
#         'lane_divider_direction_major':types.VARCHAR(20),'lane_divider_direction_minor':types.VARCHAR(20),
#         'lane_divider_main_general':types.VARCHAR(20),'lane_divider_fast_slow':types.VARCHAR(20),
#         'lane_edge_marking':types.BOOLEAN,'accident_type_major':types.VARCHAR(20),
#         'accident_type_minor':types.VARCHAR(20),'cause_analysis_major_primary':types.VARCHAR(20),
#         'cause_analysis_minor_primary':types.VARCHAR(120),'vehicle_type_major':types.VARCHAR(20),
#         'vehicle_type_minor':types.VARCHAR(20),'gender':types.VARCHAR(20),'age':types.SMALLINT,
#         'protective_equipment':types.VARCHAR(20),'mobile_device_usage':types.VARCHAR(20),
#         'party_action_major':types.VARCHAR(20),'party_action_minor':types.VARCHAR(20),
#         'impact_point_major_initial':types.VARCHAR(20),'impact_point_minor_initial':types.VARCHAR(20),
#         'impact_point_major_other':types.VARCHAR(20),'impact_point_minor_other':types.VARCHAR(20),
#         'cause_analysis_major_individual':types.VARCHAR(20),'cause_analysis_minor_individual':types.VARCHAR(100),
#         'hit_and_run':types.VARCHAR(2)
# }