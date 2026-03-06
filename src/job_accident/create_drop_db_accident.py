from sqlalchemy import create_engine, text
from src.create_table.create_accident_table import GCP_DB_BASE_URL, dbname # 從你的 config 檔匯入變數

def reset_database_environment(base_url, db_name):
    """
    強制重置資料庫環境：
    1. 刪除現有的資料庫 (Drop)
    2. 重新建立全新的資料庫 (Create)
    """
    # 連向系統層 (不指定特定 DB)
    sys_engine = create_engine(base_url)
    
    try:
        with sys_engine.connect() as conn:
            # 關閉任何可能影響 DDL 的自動事務處理
            conn.execute(text("COMMIT"))
            
            # 1. 刪除資料庫 (慎用！)
            print(f"🗑️ 正在刪除資料庫 '{db_name}'...")
            conn.execute(text(f"DROP DATABASE IF EXISTS {db_name}"))
            
            # 2. 重新建立資料庫
            print(f"🆕 正在重新建立資料庫 '{db_name}'...")
            conn.execute(text(f"CREATE DATABASE {db_name} CHARACTER SET utf8mb4"))
            
            # 強制提交變更
            conn.execute(text("COMMIT"))
            print(f"✨ 資料庫 '{db_name}' 重置完成，環境已回歸初始狀態。")
            return True
            
    except Exception as e:
        print(f"❌ 重置失敗: {e}")
        return False
    finally:
        sys_engine.dispose()

# 使用方式：
# reset_database_environment(GCP_DB_BASE_URL, dbname)
if __name__ == "__main__":
    # 這裡直接呼叫函式
    # 確保你已經從 config 匯入了 GCP_DB_BASE_URL 和 dbname
    print("⚠️ 警告：即將重置資料庫環境...")
    reset_database_environment(GCP_DB_BASE_URL, dbname)