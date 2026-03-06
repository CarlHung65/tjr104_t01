from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from src.job_accident.create_drop_db_accident import reset_database_environment
from src.create_table.create_accident_table import GCP_DB_BASE_URL, dbname

with DAG(
    dag_id='accident_db_reset_tool',
    start_date=datetime(2026, 3, 1),
    schedule=None,  # 設定為 None，表示只能手動觸發
    catchup=False,
    tags=['maintenance', 'danger_zone'],
    doc_md="## 注意：此任務會刪除並重建 Car_accident 資料庫"
) as dag:

    @task
    def run_reset():
        # 呼叫你寫好的重置函式
        success = reset_database_environment(GCP_DB_BASE_URL, dbname)
        if success:
            print(f"✅ 資料庫 {dbname} 已成功重置。")
        else:
            raise Exception("❌ 資料庫重置失敗，請檢查權限與連線。")

    run_reset()