from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'update_frontend_db_final',
    default_args=default_args,
    schedule='@monthly',
    catchup=False,
    tags=['accident_data', 'tables_only'],
) as dag:

    #### 1. 建立臨時表結構
    create_temp_skeletons = SQLExecuteQueryOperator(
        task_id='create_temp_skeletons',
        conn_id='mysql_db',
        split_statements=True,
        sql="""
        CREATE DATABASE IF NOT EXISTS frontend_db;
        USE frontend_db;
        
        -- 清除舊有的臨時表與過期備份
        DROP TABLE IF EXISTS tbl_accident_analysis_final_temp;
        DROP TABLE IF EXISTS tbl_accident_details_temp;
        DROP TABLE IF EXISTS accident_new_sq1_main_temp;
        DROP TABLE IF EXISTS accident_new_sq1_env_temp;
        DROP TABLE IF EXISTS accident_new_sq1_process_temp;

        -- 建立主分析表骨架
        CREATE TABLE tbl_accident_analysis_final_temp AS 
        SELECT m.accident_id, m.accident_datetime, m.accident_weekday AS Weekday_CN, m.latitude, m.longitude, m.death_count, m.injury_count, m.weather_condition, IFNULL(p.cause_analysis_minor_primary, '未詳查') AS primary_cause, IFNULL(h.party_action_major, '未詳查') AS party_action, YEAR(m.accident_datetime) AS Year, HOUR(m.accident_datetime) AS Hour 
        FROM car_accident.accident_sq1_main m 
        LEFT JOIN car_accident.accident_sq1_process p ON m.accident_id = p.accident_id 
        LEFT JOIN car_accident.accident_sq1_human h ON m.accident_id = h.accident_id LIMIT 0;
        
        -- 建立明細表骨架
        CREATE TABLE tbl_accident_details_temp AS 
        SELECT m.*, YEAR(m.accident_datetime) AS Year FROM car_accident.accident_sq1_main m LIMIT 0;

        -- 建立相容性資料表 (原 View 轉 Table)
        CREATE TABLE accident_new_sq1_main_temp AS SELECT * FROM car_accident.accident_sq1_main LIMIT 0;
        CREATE TABLE accident_new_sq1_env_temp AS SELECT * FROM car_accident.accident_sq1_env LIMIT 0;
        CREATE TABLE accident_new_sq1_process_temp AS SELECT * FROM car_accident.accident_sq1_process LIMIT 0;
        """
    )

    #### 2. 分年份寫入資料
    prev_task = create_temp_skeletons
    for year in range(2021, 2025):
        insert_task = SQLExecuteQueryOperator(
            task_id=f'insert_data_{year}',
            conn_id='mysql_db',
            split_statements=True,
            sql=f"""
            USE frontend_db;
            INSERT INTO tbl_accident_analysis_final_temp SELECT m.accident_id, m.accident_datetime, m.accident_weekday, m.latitude, m.longitude, m.death_count, m.injury_count, m.weather_condition, IFNULL(p.cause_analysis_minor_primary, '未詳查'), IFNULL(h.party_action_major, '未詳查'), YEAR(m.accident_datetime), HOUR(m.accident_datetime) FROM car_accident.accident_sq1_main m LEFT JOIN car_accident.accident_sq1_process p ON m.accident_id = p.accident_id LEFT JOIN car_accident.accident_sq1_human h ON m.accident_id = h.accident_id WHERE YEAR(m.accident_datetime) = {year};
            INSERT INTO tbl_accident_details_temp SELECT m.*, YEAR(m.accident_datetime) FROM car_accident.accident_sq1_main m WHERE YEAR(m.accident_datetime) = {year};
            INSERT INTO accident_new_sq1_main_temp SELECT * FROM car_accident.accident_sq1_main WHERE YEAR(accident_datetime) = {year};
            """
        )
        prev_task >> insert_task
        prev_task = insert_task

    #### 3. 補齊關聯表資料與建立索引
    post_import = SQLExecuteQueryOperator(
        task_id='post_import_processing',
        conn_id='mysql_db',
        split_statements=True,
        sql="""
        USE frontend_db;
        -- 補齊靜態關聯表
        INSERT INTO accident_new_sq1_env_temp SELECT * FROM car_accident.accident_sq1_env;
        INSERT INTO accident_new_sq1_process_temp SELECT * FROM car_accident.accident_sq1_process;

        -- 建立衍生子表
        DROP TABLE IF EXISTS tbl_pedestrian_accident_temp;
        CREATE TABLE tbl_pedestrian_accident_temp AS SELECT * FROM tbl_accident_analysis_final_temp WHERE primary_cause LIKE '%行人%' OR party_action LIKE '%行人%';
        DROP TABLE IF EXISTS tbl_accident_heatmap_temp;
        CREATE TABLE tbl_accident_heatmap_temp AS SELECT ROUND(latitude, 4) AS lat, ROUND(longitude, 4) AS lon, COUNT(*) AS count FROM tbl_accident_analysis_final_temp GROUP BY lat, lon;

        -- 建立關鍵索引
        ALTER TABLE tbl_accident_analysis_final_temp ADD INDEX idx_tmp_loc (latitude, longitude), ADD INDEX idx_tmp_year (Year);
        ALTER TABLE tbl_pedestrian_accident_temp ADD INDEX idx_tmp_ped_loc (latitude, longitude);
        ALTER TABLE tbl_accident_heatmap_temp ADD INDEX idx_tmp_heat_loc (lat, lon);
        ALTER TABLE accident_new_sq1_main_temp ADD INDEX idx_tmp_main_id (accident_id);
        """
    )

    #### 4. 瞬間更名切換 (Atomic Swap)
    atomic_swap = SQLExecuteQueryOperator(
        task_id='atomic_swap',
        conn_id='mysql_db',
        split_statements=True,
        sql="""
        USE frontend_db;
        -- 確保正式表存在 (避免初次執行 RENAME 失敗)
        CREATE TABLE IF NOT EXISTS tbl_accident_analysis_final LIKE tbl_accident_analysis_final_temp;
        CREATE TABLE IF NOT EXISTS tbl_accident_details LIKE tbl_accident_details_temp;
        CREATE TABLE IF NOT EXISTS accident_new_sq1_main LIKE accident_new_sq1_main_temp;
        CREATE TABLE IF NOT EXISTS tbl_pedestrian_accident LIKE tbl_pedestrian_accident_temp;
        CREATE TABLE IF NOT EXISTS tbl_accident_heatmap LIKE tbl_accident_heatmap_temp;

        -- 執行更名切換
        RENAME TABLE 
            tbl_accident_analysis_final TO tbl_old_main, tbl_accident_analysis_final_temp TO tbl_accident_analysis_final,
            tbl_accident_details TO tbl_old_det, tbl_accident_details_temp TO tbl_accident_details,
            accident_new_sq1_main TO tbl_old_comp, accident_new_sq1_main_temp TO accident_new_sq1_main,
            tbl_pedestrian_accident TO tbl_old_ped, tbl_pedestrian_accident_temp TO tbl_pedestrian_accident,
            tbl_accident_heatmap TO tbl_old_heat, tbl_accident_heatmap_temp TO tbl_accident_heatmap;

        -- 刪除舊表
        DROP TABLE tbl_old_main, tbl_old_det, tbl_old_comp, tbl_old_ped, tbl_old_heat;
        """
    )

    prev_task >> post_import >> atomic_swap