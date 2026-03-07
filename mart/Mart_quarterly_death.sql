-- 建立各季度各類型車禍致死人數的分析表
-- 1. 將車禍大類別分得更乾淨
CREATE OR REPLACE VIEW v_accident_sq1_process_cleaned_type_major AS
	(SELECT
		accident_id, cause_analysis_minor_individual, accident_type_major, 
		CASE
			WHEN accident_type_major = '人與汽(機)車' THEN '人與車'
			WHEN accident_type_major = '人與汽機車' THEN '人與車'
			WHEN accident_type_major = '汽(機)車本身' THEN '車輛本身'
			ELSE accident_type_major
		END AS accident_type_major_grouped
			FROM accident_sq1_process);
            
-- 2. 由於需要車禍日期、死傷人數，所以從sq1_main JOIN過來
CREATE OR REPLACE VIEW  v_accident_sq1_process_cleaned_type_major_plus_main AS
	(SELECT v.*, 
			m.accident_category, 
			m.accident_datetime, 
            m.accident_weekday,
			m.death_count, m.injury_count
		FROM v_accident_sq1_process_cleaned_type_major v
			JOIN accident_sq1_main m
				ON v.accident_id = m.accident_id);
                
-- 3. 建立Mart層圖表
CREATE TABLE IF NOT EXISTS mart_quarterly_death AS
	(SELECT
		YEAR(accident_datetime) AS `year`,
		QUARTER(accident_datetime) AS `quarter`,
		accident_type_major_grouped,
		SUM(death_count) AS `death_quarterly_total`
			FROM v_accident_sq1_process_cleaned_type_major_plus_main
				GROUP BY `year`, `quarter`, accident_type_major_grouped
					ORDER BY `year`, `quarter`);

DROP VIEW v_accident_sq1_process_cleaned_type_major;
DROP VIEW v_accident_sq1_process_cleaned_type_major_plus_main;