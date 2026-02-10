from e_crawler_accident import (auto_scrape_and_download_old_data,
                     auto_scrape_recent_data,
                     read_old_data_to_dataframe)
from t_dataclr_accident import (car_crash_old_data_clean,
                     transform_data_dict)
from l_tomysql_accident import (load_to_mysql,
                           load_to_new_mysql)
from l_tomysqlgcp_accident import (
                           load_to_GCP_mysql,
                           load_to_new_GCP_mysql,
                           load_cmp_to_new_GCP_mysql)
from l_setpkfk_accident import (
                           setting_pkfk,
                           setting_new_pkfk)
import os 
import pandas as pd
from sqlalchemy import inspect,text,create_engine
from create_accident_table import (SAVE_OLD_DATA_DIR,
                    SEQ_PAGE_URL,
                    SAVE_NEW_DATA_DIR,
                    GCP_DB_URL)
pd.set_option('future.no_silent_downcasting', True)#關閉警告

def is_db_ready(engine):
    """檢查主表是否存在且已有資料"""
    try:
        inspector = inspect(engine)
        if 'accident_sq1_main' in inspector.get_table_names():
            with engine.connect() as conn:
                count = conn.execute(text("SELECT COUNT(*) FROM accident_new_sq1_main")).scalar()
                return count > 0
    except Exception:
        return False
    return False


if __name__ == "__main__":
    print("程式開始執行...")
    engine = create_engine(GCP_DB_URL)

    if not is_db_ready(engine):
        print("📁 偵測到資料庫尚未初始化，準備匯入歷年資料...") 
        
        os.makedirs(SAVE_OLD_DATA_DIR, exist_ok=True)
        os.makedirs(SAVE_NEW_DATA_DIR, exist_ok=True)
        files = os.listdir(SAVE_OLD_DATA_DIR)
        #os.listdir這個method會去路徑下看檔案
        if len(files)>0:
            for item in files:
                full_path = os.path.join(SAVE_OLD_DATA_DIR,item)
                old_list=read_old_data_to_dataframe(full_path)
                trans=transform_data_dict(old_list)
                cleaned=car_crash_old_data_clean(trans)
                clean1 = cleaned['main']
                clean2 = cleaned['party']
                db_engine = load_to_GCP_mysql(clean1,clean2)
                #db_engine=load_to_mysql(clean1,clean2)
            if db_engine:
                setting_pkfk(db_engine)
        else:
            for i in range(len(SEQ_PAGE_URL)):
                old=auto_scrape_and_download_old_data(SEQ_PAGE_URL[i])
                trans=transform_data_dict(old)
                cleaned=car_crash_old_data_clean(trans)
                clean1 = cleaned['main']
                clean2 = cleaned['party']
                db_engine=load_to_GCP_mysql(clean1,clean2)
                #db_engine= load_to_mysql(clean1,clean2)
            if db_engine:
                setting_pkfk(db_engine)

    
    new=auto_scrape_recent_data()
    trans=transform_data_dict(new)
    cleaned=car_crash_old_data_clean(trans)
    clean1 = cleaned['main']
    clean2 = cleaned['party']
    db_engine=load_cmp_to_new_GCP_mysql(clean1,clean2)
    #db_engine= load_to_new_mysql(clean1,clean2)
    if db_engine:
        setting_new_pkfk(db_engine)
