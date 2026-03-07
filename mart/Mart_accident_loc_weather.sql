
-- 針對歷史靜態資料，只需一次建立表結構 + 索引
-- 1. concatnate 2021-2024的tmp表
CREATE OR REPLACE VIEW v_acc_apporx_loc_historical_yr AS 
	(SELECT * FROM tmp_acc_approx_loc_2021
			UNION ALL
			SELECT * FROM tmp_acc_approx_loc_2022
			UNION ALL
			SELECT * FROM tmp_acc_approx_loc_2023
			UNION ALL
			SELECT * FROM tmp_acc_approx_loc_2024
			ORDER BY accident_id);

-- 2. 將2021-2024串接的view表與weather_hourly_history做JOIN後一次性建表
DROP TABLE IF EXISTS mart_accident_loc_weather;
CREATE TABLE mart_accident_loc_weather
AS (SELECT 
		v.accident_id, w.id as historic_weather_id, 
        v.actual_accident_datetime, w.observation_datetime, 
        w.temperature_degree, w.apparent_temperature_degree, 
        w.rain_within_hour_mm, w.precipitation_mm, 
        w.visibility_m, w.weather_code, 
		v.longitude, v.latitude,
		w.longitude_round, w.latitude_round
			FROM v_acc_apporx_loc_historical_yr v
				LEFT JOIN weather_hourly_history w
					ON w.observation_datetime = v.approx_accident_datetime
					AND w.longitude_round = v.longitude_round
					AND w.latitude_round = v.latitude_round);
-- 3. 建立索引，以利查詢
ALTER TABLE mart_accident_loc_weather 
ADD INDEX idx_accident_dt (actual_accident_datetime),
ADD INDEX idx_loc_round (longitude_round, latitude_round);

DROP VIEW v_acc_apporx_loc_historical_yr;


-- 針對會長胖的2026年新資料，定期執行
-- 1. 刪除2026年全部資料、或是自行調整從哪開始刪alter
DELETE FROM mart_accident_loc_weather WHERE YEAR(actual_accident_datetime) = 2026;
-- 2. 插入2026年資料
INSERT INTO mart_accident_loc_weather
(SELECT 
		v.accident_id, w.id as historic_weather_id, 
        v.actual_accident_datetime, w.observation_datetime, 
        w.temperature_degree, w.apparent_temperature_degree, 
        w.rain_within_hour_mm, w.precipitation_mm, 
        w.visibility_m, w.weather_code, 
		v.longitude, v.latitude,
		w.longitude_round, w.latitude_round
			FROM tmp_acc_approx_loc_2026 v
				LEFT JOIN weather_hourly_now w
					ON w.observation_datetime = v.approx_accident_datetime
					AND w.longitude_round = v.longitude_round
					AND w.latitude_round = v.latitude_round);

-- DROP VIEW v_acc_apporx_loc_historical_yr;
