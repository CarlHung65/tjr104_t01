import pymysql
from pymysql.connections import Connection
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus


load_dotenv()
username = os.getenv("gcp_mysql_vm_user")
password = os.getenv("gcp_mysql_vm_passwd")
host = os.getenv("gcp_mysql_vm_host")
port = os.getenv("gcp_mysql_vm_port")
charset = os.getenv("charset", "utf8mb4")
writer = quote_plus(os.getenv("mail_address", "jessie"))


def get_conn_pymysql(database: str) -> Connection:
    conn = pymysql.connect(host=host,
                           port=int(port),
                           user=username,
                           password=password,
                           database=database,
                           charset=charset,
                           autocommit=True
                           )
    return conn


def get_engine_sqlalchemy(database: str) -> Engine:
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset={charset}",
        echo=False,  # 使用預設值，不印SQL日誌，保持乾淨輸出，生產環境適用
        pool_size=5,
        pool_pre_ping=True,  # 檢查連線有效性
        pool_recycle=300,    # 每5分鐘自動重整連線，可再調整
        connect_args={'connect_timeout': 60})
    return engine
