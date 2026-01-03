-- Migrated from DuckDB test: test/sql/join/ join with aggregate tests
-- Tests joins combined with aggregate functions

CREATE TABLE sensors(sensor_id INTEGER, sensor_name VARCHAR, "location" VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE readings(reading_id INTEGER, sensor_id INTEGER, "value" DOUBLE, reading_time TIMESTAMP, ts TIMESTAMP TIME INDEX);

INSERT INTO sensors VALUES
(1, 'TempSensor1', 'Room A', 1000), (2, 'TempSensor2', 'Room B', 2000),
(3, 'HumiditySensor1', 'Room A', 3000), (4, 'HumiditySensor2', 'Room B', 4000);

INSERT INTO readings VALUES
(1, 1, 22.5, '2023-01-01 10:00:00', 1000), (2, 1, 23.1, '2023-01-01 11:00:00', 2000),
(3, 1, 21.8, '2023-01-01 12:00:00', 3000), (4, 2, 25.3, '2023-01-01 10:00:00', 4000),
(5, 2, 26.0, '2023-01-01 11:00:00', 5000), (6, 2, 24.7, '2023-01-01 12:00:00', 6000),
(7, 3, 45.2, '2023-01-01 10:00:00', 7000), (8, 3, 46.8, '2023-01-01 11:00:00', 8000),
(9, 4, 52.1, '2023-01-01 10:00:00', 9000), (10, 4, 51.3, '2023-01-01 11:00:00', 10000);

-- Join with basic aggregation
SELECT
  s.sensor_name, s."location",
  COUNT(r.reading_id) as reading_count,
  AVG(r."value") as avg_value,
  MIN(r."value") as min_value,
  MAX(r."value") as max_value
FROM sensors s
INNER JOIN readings r ON s.sensor_id = r.sensor_id
GROUP BY s.sensor_id, s.sensor_name, s."location"
ORDER BY s.sensor_name;

-- Join with time-based aggregation
SELECT
  s."location",
  DATE_TRUNC('hour', r.reading_time) as hour_bucket,
  COUNT(*) as readings_per_hour,
  AVG(r."value") as avg_hourly_value
FROM sensors s
INNER JOIN readings r ON s.sensor_id = r.sensor_id
GROUP BY s."location", DATE_TRUNC('hour', r.reading_time)
ORDER BY s."location", hour_bucket;

-- Aggregation before join
SELECT
  s.sensor_name, s."location", agg_readings.avg_value, agg_readings.reading_count
FROM sensors s
INNER JOIN (
  SELECT sensor_id, AVG("value") as avg_value, COUNT(*) as reading_count
  FROM readings
  GROUP BY sensor_id
) agg_readings ON s.sensor_id = agg_readings.sensor_id
WHERE agg_readings.avg_value > 30.0
ORDER BY agg_readings.avg_value DESC;

-- Multiple aggregation levels with joins
SELECT
  location_summary.location,
  location_summary.sensor_count,
  location_summary.avg_readings_per_sensor,
  location_summary.location_avg_value
FROM (
  SELECT
    s."location",
    COUNT(DISTINCT s.sensor_id) as sensor_count,
    COUNT(r.reading_id) / COUNT(DISTINCT s.sensor_id) as avg_readings_per_sensor,
    ROUND(AVG(r."value"), 6) as location_avg_value
  FROM sensors s
  INNER JOIN readings r ON s.sensor_id = r.sensor_id
  GROUP BY s."location"
) location_summary
ORDER BY location_summary.location_avg_value DESC;

-- Join with aggregated conditions
SELECT
  s.sensor_name,
  high_readings.high_count,
  high_readings.avg_high_value
FROM sensors s
INNER JOIN (
  SELECT
    sensor_id,
    COUNT(*) as high_count,
    AVG("value") as avg_high_value
  FROM readings
  WHERE "value" > 25.0
  GROUP BY sensor_id
  HAVING COUNT(*) >= 2
) high_readings ON s.sensor_id = high_readings.sensor_id
ORDER BY high_readings.avg_high_value DESC;

DROP TABLE sensors;

DROP TABLE readings;
