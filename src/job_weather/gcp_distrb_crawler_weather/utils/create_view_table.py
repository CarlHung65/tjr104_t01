from sqlalchemy import text
import sys


# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到 ./utils下的模組或套件
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# 2. 在sys.path之後才進行import
from utils.create_engine_conn_tomysql import get_engine_sqlalchemy, get_conn_pymysql


def create_view_table(database: str, ddl_str: str) -> None:
    """create any view table as ddl desciptions."""

    # 準備與MySQL server的連線
    engine = get_engine_sqlalchemy(database)

    with engine.connect() as conn:
        conn.execute(text(str(ddl_str)))
        print("建立view表成功!")
    return None
