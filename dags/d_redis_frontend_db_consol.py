from datetime import timedelta, datetime, timezone
import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
from airflow import DAG
from airflow.sdk import task

load_dotenv()

# ==========================================
# 1. 建立資料庫連線 (自動讀取 .env)
# ==========================================
def get_db_engine():
    db_user = os.getenv("DB_USER", "root")
    db_pass = quote_plus(os.getenv("DB_PASS"))
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    target_db = "frontend_db_consol"
    uri = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{target_db}?charset=utf8mb4"
    return create_engine(uri, pool_pre_ping=True)

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='d_redis_frontend_db_consol',
    default_args=default_args,
    schedule='@monthly',
    start_date=datetime(2026, 3, 1, tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=['accident_data', 'tables_only'],
) as dag:

    #### 1. 跨庫合併基礎資料表 (main, env, process, human)
    # 使用 SQLExecuteQueryOperator 直接在 MySQL 內執行運算
    @task
    def merge_base_tables():
        engine = get_db_engine()
        sql_statements = """
        CREATE DATABASE IF NOT EXISTS frontend_db_consol;
        
        DROP TABLE IF EXISTS frontend_db_consol.accident_new_sq1_main_temp;
        DROP TABLE IF EXISTS frontend_db_consol.accident_new_sq1_env_temp;
        DROP TABLE IF EXISTS frontend_db_consol.accident_new_sq1_process_temp;
        DROP TABLE IF EXISTS frontend_db_consol.accident_new_sq1_human_temp;

        CREATE TABLE frontend_db_consol.accident_new_sq1_main_temp AS
        SELECT accident_id, latitude, longitude, death_count, injury_count, accident_datetime, weather_condition, accident_weekday 
        FROM car_accident.accident_sq1_main WHERE YEAR(accident_datetime) BETWEEN 2021 AND 2025
        UNION ALL
        SELECT accident_id, latitude, longitude, death_count, injury_count, accident_datetime, weather_condition, accident_weekday 
        FROM car_accident.accident_new_sq1_main WHERE YEAR(accident_datetime) = 2026;

        CREATE TABLE frontend_db_consol.accident_new_sq1_env_temp AS
        SELECT accident_id, light_condition, road_surface_condition FROM car_accident.accident_sq1_env
        UNION ALL
        SELECT accident_id, light_condition, road_surface_condition FROM car_accident.accident_new_sq1_env;

        CREATE TABLE frontend_db_consol.accident_new_sq1_process_temp AS
        SELECT accident_id, cause_analysis_major_primary, cause_analysis_minor_primary, accident_type_major FROM car_accident.accident_sq1_process
        UNION ALL
        SELECT accident_id, cause_analysis_major_primary, cause_analysis_minor_primary, accident_type_major FROM car_accident.accident_new_sq1_process;

        CREATE TABLE frontend_db_consol.accident_new_sq1_human_temp AS
        SELECT accident_id, party_action_major FROM car_accident.accident_sq1_human
        UNION ALL
        SELECT accident_id, party_action_major FROM car_accident.accident_new_sq1_human;
        """
        # 逐行切割並執行 SQL，engine.begin() 會自動 commit
        with engine.begin() as conn:
            for statement in sql_statements.split(';'):
                if statement.strip():
                    conn.execute(text(statement.strip()))
        return "merge_base_tables_done"

    #### 2. 建立分析表 和 子表 (加入髒數據清洗邏輯)
    # 建立 Denormalized (反正規化) 的分析用大表
    # 將上述 4 張表利用 accident_id 做 LEFT JOIN 橫向拼接。並在過程中用 CASE WHEN 進行標準化清理
    @task
    def build_analysis_tables(dependency):
        engine = get_db_engine()
        sql_statements = """
        DROP TABLE IF EXISTS frontend_db_consol.tbl_accident_analysis_final_temp;
        DROP TABLE IF EXISTS frontend_db_consol.tbl_accident_details_temp;
        DROP TABLE IF EXISTS frontend_db_consol.tbl_pedestrian_accident_temp;
        DROP TABLE IF EXISTS frontend_db_consol.tbl_accident_heatmap_temp;

        CREATE TABLE frontend_db_consol.tbl_accident_analysis_final_temp AS 
        SELECT 
            m.accident_id, m.accident_datetime, m.accident_weekday AS Weekday_CN, 
            m.latitude, m.longitude, m.death_count, m.injury_count, m.weather_condition, 
            IFNULL(p.cause_analysis_minor_primary, '未詳查') AS primary_cause, 
            CASE
                WHEN p.accident_type_major IN ('人與汽(機)車', '人與汽機車') THEN '人與車'
                WHEN p.accident_type_major = '汽(機)車本身' THEN '車輛本身'
                ELSE p.accident_type_major
            END AS accident_type_major,
            CASE
                WHEN p.cause_analysis_major_primary IN ('行人(或乘客)', '非駕駛者') THEN '非駕駛者(行人或乘客)'
                WHEN p.cause_analysis_major_primary IN ('駕駛人', '無(車輛駕駛者因素)') THEN '駕駛者'
                ELSE p.cause_analysis_major_primary
            END AS cause_analysis_major,
            IFNULL(h.party_action_major, '未詳查') AS party_action, 
            YEAR(m.accident_datetime) AS Year, HOUR(m.accident_datetime) AS Hour,
            e.light_condition, e.road_surface_condition
        FROM frontend_db_consol.accident_new_sq1_main_temp m 
        LEFT JOIN frontend_db_consol.accident_new_sq1_process_temp p ON m.accident_id = p.accident_id 
        LEFT JOIN frontend_db_consol.accident_new_sq1_env_temp e ON m.accident_id = e.accident_id
        LEFT JOIN frontend_db_consol.accident_new_sq1_human_temp h ON m.accident_id = h.accident_id;
        
        CREATE TABLE frontend_db_consol.tbl_accident_details_temp AS 
        SELECT m.*, YEAR(m.accident_datetime) AS Year FROM frontend_db_consol.accident_new_sq1_main_temp m;

        CREATE TABLE frontend_db_consol.tbl_pedestrian_accident_temp AS 
        SELECT * FROM frontend_db_consol.tbl_accident_analysis_final_temp 
        WHERE primary_cause LIKE '%行人%' OR party_action LIKE '%行人%';

        CREATE TABLE frontend_db_consol.tbl_accident_heatmap_temp AS 
        SELECT ROUND(latitude, 4) AS lat, ROUND(longitude, 4) AS lon, COUNT(*) AS count 
        FROM frontend_db_consol.tbl_accident_analysis_final_temp GROUP BY lat, lon;

        ALTER TABLE frontend_db_consol.tbl_accident_analysis_final_temp ADD INDEX idx_tmp_loc (latitude, longitude), ADD INDEX idx_tmp_year (Year);
        ALTER TABLE frontend_db_consol.tbl_pedestrian_accident_temp ADD INDEX idx_tmp_ped_loc (latitude, longitude);
        ALTER TABLE frontend_db_consol.tbl_accident_heatmap_temp ADD INDEX idx_tmp_heat_loc (lat, lon);
        ALTER TABLE frontend_db_consol.accident_new_sq1_main_temp ADD INDEX idx_tmp_main_id (accident_id);
        """
        with engine.begin() as conn:
            for statement in sql_statements.split(';'):
                if statement.strip():
                    conn.execute(text(statement.strip()))
        return "build_analysis_tables_done"

    #### 3. 瞬間更名切換
    # Atomic Swap達成零停機部署
    # 利用 MySQL 的 RENAME 特性，將舊表退役、新表扶正，確保前端網站不會因為 ETL 寫入耗時的過程而查詢不到資料或壞掉
    @task
    def atomic_swap(dependency):
        engine = get_db_engine()
        sql_statements = """
        CREATE TABLE IF NOT EXISTS frontend_db_consol.tbl_accident_analysis_final LIKE frontend_db_consol.tbl_accident_analysis_final_temp;
        CREATE TABLE IF NOT EXISTS frontend_db_consol.tbl_accident_details LIKE frontend_db_consol.tbl_accident_details_temp;
        CREATE TABLE IF NOT EXISTS frontend_db_consol.accident_new_sq1_main LIKE frontend_db_consol.accident_new_sq1_main_temp;
        CREATE TABLE IF NOT EXISTS frontend_db_consol.tbl_pedestrian_accident LIKE frontend_db_consol.tbl_pedestrian_accident_temp;
        CREATE TABLE IF NOT EXISTS frontend_db_consol.tbl_accident_heatmap LIKE frontend_db_consol.tbl_accident_heatmap_temp;
        CREATE TABLE IF NOT EXISTS frontend_db_consol.accident_new_sq1_env LIKE frontend_db_consol.accident_new_sq1_env_temp;
        CREATE TABLE IF NOT EXISTS frontend_db_consol.accident_new_sq1_process LIKE frontend_db_consol.accident_new_sq1_process_temp;
        CREATE TABLE IF NOT EXISTS frontend_db_consol.accident_new_sq1_human LIKE frontend_db_consol.accident_new_sq1_human_temp;

        RENAME TABLE 
            frontend_db_consol.tbl_accident_analysis_final TO frontend_db_consol.tbl_old_main, frontend_db_consol.tbl_accident_analysis_final_temp TO frontend_db_consol.tbl_accident_analysis_final,
            frontend_db_consol.tbl_accident_details TO frontend_db_consol.tbl_old_det, frontend_db_consol.tbl_accident_details_temp TO frontend_db_consol.tbl_accident_details,
            frontend_db_consol.accident_new_sq1_main TO frontend_db_consol.tbl_old_comp, frontend_db_consol.accident_new_sq1_main_temp TO frontend_db_consol.accident_new_sq1_main,
            frontend_db_consol.tbl_pedestrian_accident TO frontend_db_consol.tbl_old_ped, frontend_db_consol.tbl_pedestrian_accident_temp TO frontend_db_consol.tbl_pedestrian_accident,
            frontend_db_consol.tbl_accident_heatmap TO frontend_db_consol.tbl_old_heat, frontend_db_consol.tbl_accident_heatmap_temp TO frontend_db_consol.tbl_accident_heatmap,
            frontend_db_consol.accident_new_sq1_env TO frontend_db_consol.tbl_old_env, frontend_db_consol.accident_new_sq1_env_temp TO frontend_db_consol.accident_new_sq1_env,
            frontend_db_consol.accident_new_sq1_process TO frontend_db_consol.tbl_old_proc, frontend_db_consol.accident_new_sq1_process_temp TO frontend_db_consol.accident_new_sq1_process,
            frontend_db_consol.accident_new_sq1_human TO frontend_db_consol.tbl_old_human, frontend_db_consol.accident_new_sq1_human_temp TO frontend_db_consol.accident_new_sq1_human;

        DROP TABLE IF EXISTS frontend_db_consol.tbl_old_main, frontend_db_consol.tbl_old_det, frontend_db_consol.tbl_old_comp, frontend_db_consol.tbl_old_ped, frontend_db_consol.tbl_old_heat, frontend_db_consol.tbl_old_env, frontend_db_consol.tbl_old_proc, frontend_db_consol.tbl_old_human;
        """
        with engine.begin() as conn:
            for statement in sql_statements.split(';'):
                if statement.strip():
                    conn.execute(text(statement.strip()))
        return "atomic_swap_done"
        
    # 定義依賴順序，透過參數傳遞強制任務循序漸進
    step1 = merge_base_tables()
    step2 = build_analysis_tables(step1)
    step3 = atomic_swap(step2)