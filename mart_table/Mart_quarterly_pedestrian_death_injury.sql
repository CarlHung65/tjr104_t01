-- 建立各季度行人涉入車禍案件中傷亡人數的分析表
-- 1. 篩選車禍當事者有行人的資料列
CREATE OR REPLACE VIEW v_accident_sq1process_sq2process AS
	(SELECT * FROM accident_sq1_process
			WHERE vehicle_type_minor like "行人"
				UNION ALL
				 SELECT	* FROM accident_sq2_process
						WHERE vehicle_type_minor like "行人"
				UNION ALL
				 SELECT	* FROM accident_new_sq1_process
						WHERE vehicle_type_minor like "行人"
				UNION ALL
				 SELECT	* FROM accident_new_sq2_process
						WHERE vehicle_type_minor like "行人");

CREATE OR REPLACE VIEW v_accident_sq1process_sq2process_rn1 AS
	(SELECT tmp.* FROM 
		(SELECT *, ROW_NUMBER() OVER (PARTITION BY accident_id) AS rn
					FROM v_accident_sq1process_sq2process) tmp
			WHERE tmp.rn = 1);

-- 2. 由於需要車禍日期、死傷人數，所以從sq1_main JOIN過來
CREATE OR REPLACE VIEW v_accident_sq1main_newsq1main AS
	(SELECT * FROM  accident_new_sq1_main
		UNION ALL
		SELECT * FROM accident_sq1_main);

CREATE OR REPLACE VIEW v_accident_sq1prs_sq2prs_sq1m_newsq1m AS
	(SELECT vp.accident_id,  
            vm.accident_datetime, 
			vm.death_count, 
            vm.injury_count
		FROM v_accident_sq1process_sq2process_rn1 vp
			INNER JOIN v_accident_sq1main_newsq1main vm
				ON vp.accident_id = vm.accident_id);
                
-- 3. 建立Mart層圖表
CREATE TABLE IF NOT EXISTS mart_monthly_pedestrian_dj AS
	(SELECT
		YEAR(accident_datetime) AS `year`,
		QUARTER(accident_datetime) AS `quarter`,
		MONTH(accident_datetime) AS `month`,
		SUM(death_count) AS death_monthly_total,
		SUM(injury_count) AS injury_monthly_total
			FROM v_accident_sq1prs_sq2prs_sq1m_newsq1m
				GROUP BY `year`, `quarter`, `month`
					ORDER BY `year`, `quarter`);


ALTER TABLE mart_monthly_pedestrian_dj
	ADD COLUMN (
				avg_monthly_death_btw_2021_2023 DECIMAL(10,1), 
				stdev_monthly_death_btw_2021_2023 DECIMAL(10,1),
				avg_monthly_injury_btw_2021_2023 DECIMAL(10,1),
				stdev_monthly_injury_btw_2021_2023 DECIMAL(10,1),
				`avg+stdev_monthly_death_btw_2021_2023` DECIMAL(10,1),
				`avg-stdev_monthly_death_btw_2021_2023` DECIMAL(10,1),
				`avg+stdev_monthly_injury_btw_2021_2023` DECIMAL(10,1),
				`avg-stdev_monthly_injury_btw_2021_2023` DECIMAL(10,1)
				);

SELECT
	@avg_death := AVG(death_monthly_total),
    @stdev_death := STDDEV(death_monthly_total),
    @avg_injury := AVG(injury_monthly_total),
    @stdev_injury := STDDEV(injury_monthly_total),
	@`avg+sd_death` := AVG(death_monthly_total) + STDDEV(death_monthly_total),
    @`avg-sd_death` := AVG(death_monthly_total) - STDDEV(death_monthly_total),
    @`avg+sd_injury` := AVG(injury_monthly_total) + STDDEV(injury_monthly_total),
    @`avg-sd_injury` := AVG(injury_monthly_total) - STDDEV(injury_monthly_total)
	FROM mart_monthly_pedestrian_dj
	WHERE `year` BETWEEN 2021 AND 2023;

UPDATE mart_monthly_pedestrian_dj
	SET 
		avg_monthly_death_btw_2021_2023 = @avg_death,
		stdev_monthly_death_btw_2021_2023 = @stdev_death,
		avg_monthly_injury_btw_2021_2023 = @avg_injury,
		stdev_monthly_injury_btw_2021_2023 = @stdev_injury,
		`avg+stdev_monthly_death_btw_2021_2023` = @`avg+sd_death`,
		`avg-stdev_monthly_death_btw_2021_2023` = @`avg-sd_death`,
		`avg+stdev_monthly_injury_btw_2021_2023` = @`avg+sd_injury`,
		`avg-stdev_monthly_injury_btw_2021_2023` = @`avg-sd_injury`;


DROP VIEW  v_accident_sq1prs_sq2prs_sq1m_newsq1m;
DROP VIEW v_accident_sq1main_newsq1main;
DROP VIEW v_accident_sq1process_sq2process_rn1;
DROP VIEW v_accident_sq1process_sq2process;