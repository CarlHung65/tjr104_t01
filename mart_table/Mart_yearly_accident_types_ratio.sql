-- 建立各個年度，人車肇事案件類別的案量比例分析
-- 1. 將車禍大類別分得更乾淨，作為view表。
CREATE OR REPLACE VIEW v_accident_sq1_process_grouped_type AS
	(SELECT
		*,
		CASE
			WHEN accident_type_major = "人與汽(機)車" THEN "人與車"
			WHEN accident_type_major = "人與汽機車" THEN "人與車"
			WHEN accident_type_major = "汽(機)車本身" THEN "車輛本身"
			ELSE accident_type_major
		END AS accident_type_major_grouped
			FROM accident_sq1_process);
            
-- 2. 依照年份與車禍大類別分組後，計算案件數量，作為view表。
CREATE OR REPLACE VIEW v_accident_type_yearly AS
	(SELECT
		LEFT(accident_id, 4) AS `year`,
		accident_type_major_grouped,
		COUNT(*) AS accident_yearly_counts
			FROM v_accident_sq1_process_grouped_type
				GROUP BY `year`, accident_type_major_grouped);
                
-- 3. 計算年度佔比，存成實體資料表。
CREATE TABLE IF NOT EXISTS mart_yearly_accident_types_ratio AS
	(SELECT
		`year`,
		accident_type_major_grouped,
		accident_yearly_counts,
		accident_yearly_counts / SUM(accident_yearly_counts) OVER (
																	PARTITION BY `year`
																  ) 
															 AS accident_major_type_ratio
			FROM v_accident_type_yearly);

DROP VIEW v_accident_type_yearly;
DROP VIEW v_accident_sq1_process_grouped_type;