-- 針對與行人直接相關的車禍案件，分析第一肇事者的(它可能是行人也可能是駕駛人)的用路行為
-- 1. 先串接出第一肇事者所需的欄位：個人身份特徵與當下用路行為。然後留下主要肇因關係有包含人的。
CREATE OR REPLACE VIEW v_sq1_main_human_process AS
	(SELECT m1.accident_datetime, 
			h1.gender, 
            h1.age,
            p1.accident_type_major,
            p1.vehicle_type_major
		FROM accident_sq1_main m1
			JOIN accident_sq1_human h1
			ON m1.accident_id = h1.accident_id
			JOIN accident_sq1_process p1
			ON m1.accident_id = p1.accident_id
				WHERE p1.accident_type_major LIKE "%人" OR
					  p1.accident_type_major LIKE "%人%" OR
					  p1.accident_type_major LIKE "人%");

-- 2. 將年齡分群轉為離散。將用路行為之一：車種分群。
CREATE OR REPLACE VIEW v_sq1_main_human_process_grouped AS
	SELECT 
			YEAR(accident_datetime) AS `year`, 
            gender, 
            age,
			CASE
				WHEN age < 18 AND age > 0 THEN "< 18歲"
				WHEN age < 20 AND age >= 18 THEN "18 & 19歲"
				WHEN age < 90 AND age >= 20 THEN CONCAT(ROUND(age/10)*10, " - ", 
														(ROUND(age/10)*10+9), "歲")
				ELSE "未知"
			END AS age_grouped,
			
			CASE
				WHEN vehicle_type_major LIKE "小客車" THEN "小客車(含客、貨兩用)"
				WHEN vehicle_type_major LIKE "小貨車" THEN "小貨車(含客、貨兩用)"
				ELSE vehicle_type_major 
			END AS vehicle_type_grouped,
			
			CASE
				WHEN age < 18 THEN "未成年"
				WHEN age >= 18 AND age < 65 THEN "青壯年者"
				WHEN age >= 65 AND age < 90 THEN "銀髮族"
				ELSE "未知"
			END AS age_cluster,
			
			CASE
				WHEN accident_type_major = '人與汽(機)車' THEN '人與車'
				WHEN accident_type_major = '人與汽機車' THEN '人與車'
				ELSE accident_type_major
			END AS accident_type_major_grouped
		FROM v_sq1_main_human_process v;

-- 3. 逐年計算各年齡區間的平均年齡(男女分開計算)、各車種駕駛人平均年齡(各年齡層分開計算)，正式存成Mart層
CREATE TABLE IF NOT EXISTS mart_yearly_partyA_road_user_features AS
	WITH partitioned_avgs AS (
		SELECT 
				`year`,
				gender, 
				age_grouped, 
				age_cluster,
				vehicle_type_grouped, 
				accident_type_major_grouped,
				AVG(age) OVER (PARTITION BY `year`, 
											gender, 
											age_grouped
							  ) as `各年度各年齡區間的平均年齡(男女分開計算)`,
				AVG(age) OVER (PARTITION BY `year`, 
											age_grouped, 
											vehicle_type_grouped
							  ) as `各年度各車種駕駛人平均年齡(各年齡層分開計算)`
			FROM v_sq1_main_human_process_grouped
				WHERE age_grouped != "未知" AND accident_type_major_grouped = "人與車"
		  )
	SELECT * FROM partitioned_avgs;

DROP VIEW v_sq1_main_human_process;
DROP VIEW v_sq1_main_human_process_grouped;