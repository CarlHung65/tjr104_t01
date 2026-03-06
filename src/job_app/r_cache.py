import os
import redis
import pickle # 把 DataFrame 壓縮成二進位存入 Redis
from dotenv import load_dotenv

load_dotenv()

# 初始化 Redis
# 1. 決定 host 地址
host = os.getenv("REDIS_HOST")
if not host:
    if os.getenv("AIRFLOW_HOME"):
        host = "redis"      # 如果是 Airflow (在 Docker 內)，固定找 redis 容器
    else:
        host = "127.0.0.1"  # 如果是 Streamlit (在本機端)，固定找 127.0.0.1 (比 localhost 更穩)

# 2. 將變數代入初始化
REDIS_POOL = redis.ConnectionPool(
    host=host,
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD", "123456"),
    decode_responses=False
)

"""從 Redis 取得資料"""
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

"""將資料寫入 Redis (預設存活 1 小時)"""
def set_cache(key, value, ttl=3600):
    try:
        r = redis.Redis(connection_pool=REDIS_POOL)
        packed_data = pickle.dumps(value) # 壓縮成二進位
        r.setex(key, ttl, packed_data)
        print(f"[Redis Saved] 已快取: {key}")
    except Exception as e:
        print(f"Redis 寫入失敗: {e}")


