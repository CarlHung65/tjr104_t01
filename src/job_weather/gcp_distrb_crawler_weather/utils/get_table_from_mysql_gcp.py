import pandas as pd
from sqlalchemy import text
import sys


# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到 ./utils下的模組或套件
if '/opt/airflow' not in sys.path:
    sys.path.append('/opt/airflow')

# 2. 在sys.path之後才進行import
from utils.create_engine_conn_tomysql import get_engine_sqlalchemy, get_conn_pymysql, writer


def get_table_from_sqlserver(database: str, dql_str: str) -> pd.DataFrame:
    """Read the desingated table in MySQL server. """

    # 準備與GCP VM上的MySQL server的連線
    engine = get_engine_sqlalchemy(database)

    with engine.connect() as conn:
        result = conn.execute(text(str(dql_str)))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df
