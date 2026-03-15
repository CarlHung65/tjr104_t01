import os
import redis
import pickle # 把 DataFrame 壓縮成二進位存入 Redis
from dotenv import load_dotenv

load_dotenv()

# 初始化 Redis
# 自動辨識執行環境
# 利用 AIRFLOW_HOME 環境變數判斷程式是跑在 Docker 內部還是本機端
# 讓同一套程式碼可以無縫在 ETL (Airflow) 與前端 (Streamlit) 之間共用

# 1. 決定 host 地址
if os.getenv("AIRFLOW_HOME"):
    host = "redis"      # 如果是 Airflow (在 Docker 內)，固定找 redis 容器
else:
    host = "127.0.0.1"  # 如果是 Streamlit (在本機端)，固定找 127.0.0.1 (比 localhost 更穩)

# 2. 將變數代入初始化
# decode_responses=False 是關鍵設定。因為要存入 Pickle 二進位資料(DataFrame)
# 如果設為 True，Redis 會強制轉為字串，導致二進位資料損壞。
REDIS_POOL = redis.ConnectionPool(
    host=host,
    port=int(os.getenv("REDIS_PORT")),
    password=os.getenv("REDIS_PASSWORD"),
    decode_responses=False)

"""從 Redis 取得資料"""
# 嘗試從 Redis 讀取並反序列化(Unpickle) 回復成原本的 Pandas DataFrame。
def get_cache(key):
    try:
        r = redis.Redis(connection_pool=REDIS_POOL)
        data = r.get(key)
        if data:
            print(f"Redis 讀取成功: {key}")
            return pickle.loads(data) # 解壓縮還原成 DataFrame/Dict
    except Exception as e:
        print(f"Redis 讀取失敗: {e}")
    return None

"""將資料寫入 Redis """
# 將 DataFrame 序列化並寫入 Redis
# 預設 ttl=864000 (10天)，作為熱資料的有效期限，過期會自動清除以釋放記憶體
def set_cache(key, value, ttl=864000):
    try:
        r = redis.Redis(connection_pool=REDIS_POOL)
        packed_data = pickle.dumps(value) # 壓縮成二進位
        r.setex(key, ttl, packed_data)
        print(f"[Redis Saved] 已快取: {key}")
    except Exception as e:
        print(f"Redis 寫入失敗: {e}")