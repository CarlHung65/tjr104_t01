
-- 針對歷史靜態資料，只需一次建立表結構 + 索引
-- 1. 將2021-2024的bridge表與weather_hourly_history做JOIN後一次性建表
DROP TABLE IF EXISTS mart_weather_at_accident_locations;
CREATE TABLE mart_weather_at_accident_locations
	AS (SELECT 
			bg.accident_id, w.*
				FROM accident_weather_bridge_history bg
					LEFT JOIN weather_hourly_history w
						ON bg.weather_record_id = w.weather_record_id);
						
-- 2. 建立索引，以利查詢
ALTER TABLE mart_weather_at_accident_locations
ADD INDEX idx_martaw_obsdt (observation_datetime),
ADD INDEX idx_martaw_roundedloc (longitude_round, latitude_round),
ADD INDEX idx_martaw_accID (accident_id); 

-- 針對會長胖的2026年新資料，定期執行
-- 1. 刪除2026年全部資料、或是自行調整從哪開始刪
DELETE FROM mart_weather_at_accident_locations WHERE YEAR(observation_datetime) = 2026;

-- 2. 插入2026年資料
INSERT INTO mart_weather_at_accident_locations
	SELECT 
		bg.accident_id, w.*
			FROM accident_weather_bridge_now bg
				LEFT JOIN weather_hourly_now w
					ON bg.weather_record_id = w.weather_record_id;