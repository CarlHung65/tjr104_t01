-- 針對與行人直接相關的車禍案件，分析主要肇因比例分析表
-- 1. 將車禍大類別與用路人身份大類別 清得更乾淨。
CREATE OR REPLACE VIEW v_accident_sq1_process_grouped_types AS
	(SELECT
		accident_id, 
        cause_analysis_major_primary,
        cause_analysis_minor_primary,
        accident_type_major, 
		CASE
			WHEN accident_type_major = "人與汽(機)車" THEN "人與車"
			WHEN accident_type_major = "人與汽機車" THEN "人與車"
			WHEN accident_type_major = "汽(機)車本身" THEN "車輛本身"
			ELSE accident_type_major
		END AS accident_type_major_grouped,
        
		CASE
			WHEN cause_analysis_major_primary = "行人(或乘客)" THEN "非駕駛者(行人或乘客)"
            WHEN cause_analysis_major_primary = "非駕駛者" THEN "非駕駛者(行人或乘客)"
            WHEN cause_analysis_major_primary = "駕駛人" THEN "駕駛者"
            WHEN cause_analysis_major_primary = "無(車輛駕駛者因素)" THEN "駕駛者"
            ELSE cause_analysis_major_primary            
		END AS cause_analysis_major_primary_grouped
			FROM accident_sq1_process);
            
-- 2. 由於需要車禍日期、死傷人數，所以從sq1_main JOIN過來，JOIN後只留下"人與車"這類與行人直接相關的車禍案件
CREATE OR REPLACE VIEW v_accident_sq1_process_grouped_types_sq1m AS
	(SELECT v.*, 
			m.accident_category, 
            m.accident_datetime
		FROM v_accident_sq1_process_grouped_types v
			JOIN accident_sq1_main m
			ON v.accident_id = m.accident_id
				WHERE accident_type_major_grouped = "人與車");

                
-- 3. 建立Mart層圖表
CREATE TABLE IF NOT EXISTS mart_pedestrian_related_causes_top5 AS
	-- 使用Common Table Expression語法生成臨時資料表，並宣告為ranked_behaviors資料表
	WITH ranked_behaviors AS 
			(SELECT
						YEAR(accident_datetime) AS `year`,
						QUARTER(accident_datetime) AS `quarter`,
						cause_analysis_major_primary_grouped AS `type of road user`,
						cause_analysis_minor_primary AS `behavior`,
						COUNT(cause_analysis_minor_primary) as `counts of behavior`,
						-- RANK OVER()內放窗口內的運算邏輯
						RANK() OVER (
									  PARTITION BY YEAR(accident_datetime), QUARTER(accident_datetime)
									  ORDER BY COUNT(cause_analysis_minor_primary) DESC
									) as `rank`
				FROM v_accident_sq1_process_grouped_types_sq1m
					GROUP BY `year`, `quarter`, `type of road user`, `behavior`
			  )
		-- 主查詢區:
		SELECT `year`, `quarter`, `type of road user`, `behavior`, `counts of behavior`, `rank`
		FROM ranked_behaviors
		WHERE `rank` <= 5
		ORDER BY `year`, `quarter`, `rank`;

DROP VIEW v_accident_sq1_process_grouped_types;
DROP VIEW v_accident_sq1_process_grouped_types_sq1m;