from e_crawler_accident import (auto_scrape_and_download_old_data)
from t_dataclr_accident import (car_crash_old_data_clean,transform_data_dict)
from l_tomysqlgcp_accident import (load_to_GCP_mysql_tmp_table)
from l_setpkfk_accident import (setting_TMP_pkfk)
import gc
import os 
import pandas as pd
from sqlalchemy import inspect,text,create_engine
from src.create_table.create_accident_table import (ONE_PAGE_URL,GCP_DB_URL)
pd.set_option('future.no_silent_downcasting', True)#關閉警告

if __name__ == "__main__":
    print("程式開始執行...")
    engine = create_engine(GCP_DB_URL)   
    old=auto_scrape_and_download_old_data(ONE_PAGE_URL)
    trans=transform_data_dict(old)
    cleaned=car_crash_old_data_clean(trans)
    clean1 = cleaned['main']
    clean2 = cleaned['party']
    db_engine=load_to_GCP_mysql_tmp_table(clean1,clean2)
    del old, trans, cleaned, clean1, clean2
    gc.collect()  # 手動啟動垃圾回收
    print("年度匯入完成，開始統一建立資料庫關聯 (PK/FK)...")
    setting_TMP_pkfk(engine)
