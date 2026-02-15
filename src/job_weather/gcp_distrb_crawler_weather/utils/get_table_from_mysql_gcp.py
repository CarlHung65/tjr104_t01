import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus


def get_table_from_sqlserver(database: str, dql_str: str) -> pd.DataFrame:
    """Read the desingated table in MySQL server. """

    # 準備與GCP VM上的MySQL server的連線
    load_dotenv()
    username = os.getenv("gcp_mysql_vm_user")
    password = os.getenv("gcp_mysql_vm_passwd")
    host = os.getenv("gcp_mysql_vm_host")
    port = os.getenv("gcp_mysql_vm_port")
    charset = os.getenv("charset", "utf8mb4")
    db_name = database

    # 如.env沒有mail_adress，以jessie為預設
    writer = quote_plus(os.getenv("mail_address", "jessie"))

    # {host}:{port}應該要是 localhost:3307? 、 127.0.0.1:3307? 、mysql:3307?
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{host}:{port}/{db_name}?charset={charset}",
        echo=False,  # 使用預設值，不印SQL日誌，保持乾淨輸出，生產環境適用
        pool_pre_ping=True,  # 檢查連線有效性
        pool_recycle=300,    # 每5分鐘自動重整連線，可再調整
        connect_args={'connect_timeout': 60})

    # 極度詭異區，要做點複習。
    with engine.connect() as conn:
        result = conn.execute(text(str(dql_str)))
        # 把 ResultProxy 轉換成 DataFrame
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df
