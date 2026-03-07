-- 針對與行人直接相關的車禍案件，分析主要肇因比例分析表
-- 1. 將車禍大類別與用路人身份大類別 清得更乾淨。
CREATE OR REPLACE VIEW v_accident_sq1_process_cleaned_type_major AS
	(SELECT
		accident_id, 
        cause_analysis_major_individual,
        cause_analysis_minor_individual, 
        accident_type_major, 
		CASE
			WHEN accident_type_major = "人與汽(機)車" THEN "人與車"
			WHEN accident_type_major = "人與汽機車" THEN "人與車"
			WHEN accident_type_major = "汽(機)車本身" THEN "車輛本身"
			ELSE accident_type_major
		END AS accident_type_major_grouped,
        
		CASE
			WHEN cause_analysis_major_individual = "行人(或乘客)" THEN "非駕駛者(行人或乘客)"
            WHEN cause_analysis_major_individual = "非駕駛者" THEN "非駕駛者(行人或乘客)"
            WHEN cause_analysis_major_individual = "駕駛人" THEN "駕駛者"
            WHEN cause_analysis_major_individual = "無(車輛駕駛者因素)" THEN "駕駛者"
            ELSE cause_analysis_major_individual
            
		END AS cause_analysis_major_individual_grouped
			FROM accident_sq1_process);
            
-- 2. 由於需要車禍日期、死傷人數，所以從sq1_main JOIN過來，JOIN後只留下"人與車"這類與行人直接相關的車禍案件
CREATE OR REPLACE VIEW  v_accident_sq1_process_cleaned_type_major_plus_main AS
	(SELECT v.*, 
			m.accident_category, 
            m.accident_datetime
		FROM v_accident_sq1_process_cleaned_type_major v
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
						cause_analysis_major_individual_grouped AS `type of road user`,
						cause_analysis_minor_individual AS `behavior`,
						COUNT(cause_analysis_minor_individual) as `counts of behavior`,
						-- RANK OVER()內放窗口內的運算邏輯
						RANK() OVER (
									  PARTITION BY YEAR(accident_datetime), QUARTER(accident_datetime)
									  ORDER BY COUNT(cause_analysis_minor_individual) DESC
									) as `rank`
				FROM v_accident_sq1_process_cleaned_type_major_plus_main
					GROUP BY `year`, `quarter`, `type of road user`, `behavior`
			  )
		-- 主查詢區:
		SELECT `year`, `quarter`, `type of road user`, `behavior`, `counts of behavior`, `rank`
		FROM ranked_behaviors
		WHERE `rank` <= 5
		ORDER BY `year`, `quarter`, `rank`;

DROP VIEW v_accident_sq1_process_cleaned_type_major;
DROP VIEW v_accident_sq1_process_cleaned_type_major_plus_main;



# 發現"cause_analysis_minor_individual"登記習慣有變化的過程:
-- WITH `2023年Q2以前行人用路行為` AS
-- 	(SELECT distinct cause_analysis_minor_individual as `2023年Q2以前行人用路行為類型`
-- 	FROM  v_accident_sq1_process_cleaned_type_major_plus_main
-- 		WHERE YEAR(accident_datetime) = 2023 AND MONTH(accident_datetime) < 7 AND cause_analysis_minor_individual like '%穿越%'),
-- 	 `2023年Q2(含)起行人用路行為` AS
-- 	(SELECT distinct cause_analysis_minor_individual as `2023年Q2(含)起行人用路行為類型`
-- 		FROM v_accident_sq1_process_cleaned_type_major_plus_main
-- 			WHERE YEAR(accident_datetime) = 2023 AND MONTH(accident_datetime) >= 7 AND cause_analysis_minor_individual like '%穿越%')
-- 	SELECT t1.*, t2.*
-- 		FROM `2023年Q2以前行人用路行為` t1
-- 			RIGHT JOIN `2023年Q2(含)起行人用路行為` t2
-- 				ON t1.`2023年Q2以前行人用路行為類型` = t2.`2023年Q2(含)起行人用路行為類型`;

# 推論
# '未依標誌、標線、號誌或手勢指揮穿越道路' 於2023Q2起已經拆為 '未依標誌或標線穿越道路'&'未依號誌或手勢指揮(示)穿越道路'
# '搶越行人穿越道' 已經精確定義成 '未依標誌或標線穿越道路'或'未依號誌或手勢指揮(示)穿越道路'
# '未依規定行走行人穿越道、地下道、天橋而穿越道路' 已經簡化為 '未依規定行走地下道、天橋穿越道路'