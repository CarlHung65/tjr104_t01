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