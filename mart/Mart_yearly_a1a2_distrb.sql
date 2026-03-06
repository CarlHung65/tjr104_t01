-- 創立各年度A1A2比例分佈分析資料表
-- 1. 先建立年度基礎統計 A1/A2比例
CREATE OR REPLACE VIEW v_accident_yearly_counts AS
	(SELECT
		YEAR(accident_datetime) AS `year`,
		accident_category,
		COUNT(*) AS accident_counts,
		COUNT(*) / SUM( COUNT(*)) OVER (
										PARTITION BY 
										YEAR(accident_datetime)
									    ) AS category_ratio
			FROM accident_sq1_main
				GROUP BY `year`, accident_category);

-- 2. 再建立逐年累計事故案件數量 
CREATE OR REPLACE VIEW v_accident_yearly_running_counts AS
	(SELECT 
		`year`,
		SUM(accident_counts) AS yearly_total,
		SUM(SUM(accident_counts)) OVER (
										ORDER BY `year`
										ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                                        ) AS running_total
			FROM v_accident_yearly_counts
				GROUP BY `year`);


-- 3. 將兩張表合併, 改建table
CREATE TABLE IF NOT EXISTS mart_yearly_a1a2_distrb AS
	(SELECT v1.*, v2.yearly_total, v2.running_total
		FROM v_accident_yearly_counts v1
				JOIN v_accident_yearly_running_counts v2
					ON v1.year = v2.year);

DROP VIEW v_accident_yearly_counts;
DROP VIEW v_accident_yearly_running_counts;