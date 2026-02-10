from sqlalchemy import create_engine  # 負責：建立資料庫連線引擎，是 Python 與 SQL 的橋樑
from sqlalchemy import text,inspect,VARCHAR
from sqlalchemy import types #types用在python裡定義mysql的資料型別
import pymysql  # 代碼中沒直接用到它的 method，但它是底層驅動
import pandas as pd  # 負責：資料處理，把 CSV 轉成表格，進行切分與清洗
from create_accident_table import (
                    GCP_DB_URL,
                    MAIN_TABLE_DICT as MTD,
                    ENVIRONMENT_TABLE_DICT as ETD,
                    HUMAN_BEAHAVIOR_DICT as HBD,
                    EVENT_PROCESS_PARTICIPATE_OBJECT_DICT as EPPOD,
                    EVENT_RESULT_DICT as ERD)

#----------------------------------------------------------------
# 匯入 gcp mysql 
# ------------------------------------------------------------------
def load_to_GCP_mysql(main_dict, party_dict):
    print(f"--- 階段三：匯入 gcp MySQL ---")
    engine = create_engine(GCP_DB_URL,pool_pre_ping=True,  # 核心：確保連線有效
                           pool_recycle=300,                # 每 5 分鐘強制重整連線
                           connect_args={'connect_timeout': 60})
    
        # 寫入主表
    try:
        with engine.begin() as connection:
        # 使用 Transaction 確保資料完整性
            main_dict['master'].to_sql('accident_sq1_main', con=connection, if_exists='append', index=False,dtype= MTD,chunksize=200)
            main_dict['env'].to_sql('accident_sq1_env', con=connection, if_exists='append', index=False,dtype= ETD,chunksize=200)
            main_dict['human'].to_sql('accident_sq1_human', con=connection, if_exists='append', index=False,dtype= HBD,chunksize=200)
            main_dict['process'].to_sql('accident_sq1_process', con=connection, if_exists='append', index=False,dtype= EPPOD,chunksize=200)
            main_dict['result'].to_sql('accident_sq1_res', con=connection, if_exists='append', index=False,dtype= ERD,chunksize=200)

                # 寫入細節表

            party_dict['master'].to_sql('accident_sq2_sub', con=connection, if_exists='append', index=False,dtype= MTD,chunksize=200)
            party_dict['env'].to_sql('accident_sq2_env', con=connection, if_exists='append', index=False,dtype= ETD,chunksize=200)
            party_dict['human'].to_sql('accident_sq2_human', con=connection, if_exists='append', index=False,dtype= HBD,chunksize=200)
            party_dict['process'].to_sql('accident_sq2_process', con=connection, if_exists='append', index=False,dtype= EPPOD,chunksize=200)
            party_dict['result'].to_sql('accident_sq2_res', con=connection, if_exists='append', index=False,dtype= ERD,chunksize=200)
            #要加chunksize=200,不然上傳雲端會卡住(一次上傳500列資料)
        print("所有資料已成功寫入資料庫！")
        
        return engine
    except Exception as e:
        print(f"匯入失敗: {e}")
        return None

#----------------------------------------------------------------
# 匯入 近年資料 gcp mysql
# ------------------------------------------------------------------
def load_to_new_GCP_mysql(main_dict, party_dict):
    print(f"--- 階段三：匯入 gcp MySQL ---")
    engine = create_engine(GCP_DB_URL,pool_pre_ping=True,  # 核心：確保連線有效
                           pool_recycle=300,                # 每 5 分鐘強制重整連線
                           connect_args={'connect_timeout': 60})
    
        # 寫入主表
    try:
        with engine.begin() as connection:
        # 使用 Transaction 確保資料完整性
            main_dict['master'].to_sql('accident_new_sq1_main', con=connection, if_exists='append', index=False,dtype= MTD,chunksize=200)
            main_dict['env'].to_sql('accident_new_sq1_env', con=connection, if_exists='append', index=False,dtype= ETD,chunksize=200)
            main_dict['human'].to_sql('accident_new_sq1_human', con=connection, if_exists='append', index=False,dtype= HBD,chunksize=200)
            main_dict['process'].to_sql('accident_new_sq1_process', con=connection, if_exists='append', index=False,dtype= EPPOD,chunksize=200)
            main_dict['result'].to_sql('accident_new_sq1_res', con=connection, if_exists='append', index=False,dtype= ERD,chunksize=200)

                # 寫入細節表

            party_dict['master'].to_sql('accident_new_sq2_sub', con=connection, if_exists='append', index=False,dtype= MTD,chunksize=200)
            party_dict['env'].to_sql('accident_new_sq2_env', con=connection, if_exists='append', index=False,dtype= ETD,chunksize=200)
            party_dict['human'].to_sql('accident_new_sq2_human', con=connection, if_exists='append', index=False,dtype= HBD,chunksize=200)
            party_dict['process'].to_sql('accident_new_sq2_process', con=connection, if_exists='append', index=False,dtype= EPPOD,chunksize=200)
            party_dict['result'].to_sql('accident_new_sq2_res', con=connection, if_exists='append', index=False,dtype= ERD,chunksize=200)
            #要加chunksize=200,不然上傳雲端會卡住(一次上傳500列資料)
        print("所有資料已成功寫入資料庫！")
        
        return engine
    except Exception as e:
        print(f"匯入失敗: {e}")
        return None

#----------------------------------------------------------------
# 匯入 近年資料 gcp mysql 含比較上一筆資料
# ------------------------------------------------------------------
def load_cmp_to_new_GCP_mysql(main_dict, party_dict):
    print(f"--- 階段三：匯入 gcp MySQL ---")
    engine = create_engine(GCP_DB_URL,pool_pre_ping=True,  # 核心：確保連線有效
                           pool_recycle=300,                # 每 5 分鐘強制重整連線
                           connect_args={'connect_timeout': 60})
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    tables_to_sync = [
        (main_dict['master'], 'accident_new_sq1_main', 'tmp_main'),
        (main_dict['env'], 'accident_new_sq1_env', 'tmp_env'),
        (main_dict['human'], 'accident_new_sq1_human', 'tmp_human'),
        (main_dict['process'], 'accident_new_sq1_process', 'tmp_process'),
        (main_dict['result'], 'accident_new_sq1_res', 'tmp_result'),
        (party_dict['master'], 'accident_new_sq2_sub', 'tmp_sub_main'),
        (party_dict['env'], 'accident_new_sq2_env', 'tmp_sub_env'),
        (party_dict['human'], 'accident_new_sq2_human', 'tmp_sub_human'),
        (party_dict['process'], 'accident_new_sq2_process', 'tmp_sub_process'),
        (party_dict['result'], 'accident_new_sq2_res', 'tmp_sub_result')
    ]

    try:
        with engine.begin() as connection:
            for df, final_table, tmp_table in tables_to_sync:
                
                # --- 情況 A：表不存在 ---
                if final_table not in existing_tables:
                    print(f"检测到新表 {final_table}，正在初始化結構...")
                    # 這裡只負責「建立表」並「匯入第一次資料」
                    # 為了避免 1170 錯誤，我們還是得在建表時指定 VARCHAR，否則後續設 PK 會失敗
                    df.to_sql(final_table, con=connection, if_exists='fail', index=False, 
                              dtype={'accident_id': VARCHAR(16)})
                    print(f"⚠️ {final_table} 已建立，請記得執行 setting_new_pkfk 設定結構。")
                
                # --- 情況 B：表已存在 ---
                else:
                    print(f"更新資料表: {final_table}")
                    # 1. 寫入暫存
                    df.to_sql(tmp_table, con=connection, if_exists='replace', index=False, chunksize=200)

                    # 2. 生成更新語句 (排除 accident_id)
                    columns = [f"`{c}`" for c in df.columns if c != 'accident_id']
                    update_stmt = ", ".join([f"{c}=VALUES({c})" for c in columns])

                    # 3. 執行 UPSERT (這行生效的前提是該表已有 PK)
                    upsert_sql = f"""
                        INSERT INTO {final_table} ({", ".join([f"`{c}`" for c in df.columns])})
                        SELECT * FROM {tmp_table}
                        ON DUPLICATE KEY UPDATE {update_stmt};
                    """
                    connection.execute(text(upsert_sql))
                    connection.execute(text(f"DROP TABLE {tmp_table};"))
            
            print("✨ 增量更新完成！")
        return engine
    except Exception as e:
        print(f"匯入失敗: {e}")
        return None