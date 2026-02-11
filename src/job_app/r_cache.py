# c_cache.py
import os
import redis
import pickle # 把 DataFrame 壓縮成二進位存入 Redis
from dotenv import load_dotenv

load_dotenv()

# 初始化 Redis
REDIS_POOL = redis.ConnectionPool(
    host=os.getenv("redis_host", "localhost"),
    port=int(os.getenv("redis_port", 6379)),
    password=os.getenv("redis_password", None),
    decode_responses=False ) # 設定為 False 以便存取二進位資料 (pickle)

"""嘗試從 Redis 取得資料"""
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