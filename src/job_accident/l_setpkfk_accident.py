from sqlalchemy import text
from sqlalchemy import types #types用在python裡定義mysql的資料型別
import pymysql  # 代碼中沒直接用到它的 method，但它是底層驅動
import pandas as pd  # 負責：資料處理，把 CSV 轉成表格，進行切分與清洗
from create_accident_table import (DB_URL,
                    GCP_DB_URL,
                    MAIN_TABLE_DICT as MTD,
                    ENVIRONMENT_TABLE_DICT as ETD,
                    HUMAN_BEAHAVIOR_DICT as HBD,
                    EVENT_PROCESS_PARTICIPATE_OBJECT_DICT as EPPOD,
                    EVENT_RESULT_DICT as ERD)



# ------------------------------------------------------------------
# 設定pk fk
# ------------------------------------------------------------------
def setting_pkfk(engine):
    if engine is None: return
    
    #sub_tables = ['accident_sq2_sub']
    sub_tables = ['accident_sq1_env', 'accident_sq1_human', 'accident_sq1_process', 'accident_sq1_res',
                  'accident_sq2_sub','accident_sq2_env', 'accident_sq2_human', 'accident_sq2_process', 'accident_sq2_res']    
    with engine.connect() as conn:   
        check_pk = conn.execute(text("""
                SELECT count(*) FROM information_schema.TABLE_CONSTRAINTS 
                WHERE CONSTRAINT_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'accident_sq1_main' 
                AND CONSTRAINT_TYPE = 'PRIMARY KEY'
                """)).scalar()

        if check_pk == 0:
            try:
                conn.execute(text("ALTER TABLE accident_sq1_main MODIFY COLUMN accident_id VARCHAR(16) NOT NULL"))
                conn.execute(text("ALTER TABLE accident_sq1_main ADD PRIMARY KEY (accident_id)"))
                conn.commit() # 確保執行成功
                print("MySQL Primary Key 設定完成！")
            except Exception as e:
                print(f"PK 可能已存在或設定失敗: {e}")
            for table in sub_tables:
                try:
                    print(f"正在為 {table} 設定外鍵...")
                    # 統一修改欄位屬性
                    conn.execute(text(f"ALTER TABLE {table} MODIFY COLUMN accident_id VARCHAR(16) NOT NULL"))
                    # 統一建立 FK 連結到主表
                    conn.execute(text(f"""
                        ALTER TABLE {table} 
                        ADD CONSTRAINT fk_{table}_main 
                        FOREIGN KEY (accident_id) 
                        REFERENCES accident_sq1_main(accident_id)
                        ON DELETE CASCADE
                        """))
                    conn.commit()
                    print(f"{table} fk已設立")
                except Exception as e:
                        print(f"FK 可能已存在或設定失敗: {e}")
        else:
            print("✅ 資料庫結構PK/FK已存在,無需重複設定。")




# ------------------------------------------------------------------
# 設定 new pk fk
# ------------------------------------------------------------------
def setting_new_pkfk(engine):
    if engine is None: return
    
    sub_tables = ['accident_new_sq1_env', 'accident_new_sq1_human', 'accident_new_sq1_process', 'accident_new_sq1_res',
                  'accident_new_sq2_sub','accident_new_sq2_env', 'accident_new_sq2_human', 'accident_new_sq2_process', 'accident_new_sq2_res']    
    
    with engine.connect() as conn:   
        # --- 1. 處理主表 PK ---
        check_pk = conn.execute(text("""
                SELECT count(*) FROM information_schema.TABLE_CONSTRAINTS 
                WHERE CONSTRAINT_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'accident_new_sq1_main' 
                AND CONSTRAINT_TYPE = 'PRIMARY KEY'
                """)).scalar()

        if check_pk == 0:
            try:
                conn.execute(text("ALTER TABLE accident_new_sq1_main MODIFY COLUMN accident_id VARCHAR(16) NOT NULL"))
                conn.execute(text("ALTER TABLE accident_new_sq1_main ADD PRIMARY KEY (accident_id)"))
                conn.commit()
                print("MySQL Primary Key 設定完成！")
            except Exception as e:
                print(f"PK 設定失敗: {e}")
        else:
            print("✅ 主表 PK 已存在。")

        # --- 2. 處理細節表 FK (移動到 if 外層，確保一定會跑) ---
        for table in sub_tables:
            # 檢查這張表是否已經有 FK
            check_fk = conn.execute(text(f"""
                SELECT count(*) FROM information_schema.TABLE_CONSTRAINTS 
                WHERE CONSTRAINT_SCHEMA = DATABASE() 
                AND TABLE_NAME = '{table}' 
                AND CONSTRAINT_TYPE = 'FOREIGN KEY'
            """)).scalar()

            if check_fk == 0:
                try:
                    print(f"正在為 {table} 設定外鍵...")
                    conn.execute(text(f"ALTER TABLE {table} MODIFY COLUMN accident_id VARCHAR(16) NOT NULL"))
                    conn.execute(text(f"""
                        ALTER TABLE {table} 
                        ADD CONSTRAINT fk_{table}_main 
                        FOREIGN KEY (accident_id) 
                        REFERENCES accident_new_sq1_main(accident_id)
                        ON DELETE CASCADE
                    """))
                    conn.commit()
                    print(f"✅ {table} FK 已設立")
                except Exception as e:
                    print(f"❌ {table} FK 設定失敗: {e}")
            else:
                print(f"✅ {table} FK 已存在，跳過。")