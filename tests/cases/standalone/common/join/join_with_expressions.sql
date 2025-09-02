-- Migrated from DuckDB test: test/sql/join/ expression join tests
-- Tests joins with complex expressions

CREATE TABLE measurements(measure_id INTEGER, sensor_id INTEGER, reading DOUBLE, measure_time TIMESTAMP, ts TIMESTAMP TIME INDEX);

CREATE TABLE sensor_config(sensor_id INTEGER, min_threshold DOUBLE, max_threshold DOUBLE, calibration_factor DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO measurements VALUES 
(1, 1, 22.5, '2023-01-01 10:00:00', 1000), (2, 1, 45.8, '2023-01-01 10:05:00', 2000),
(3, 2, 18.2, '2023-01-01 10:00:00', 3000), (4, 2, 89.7, '2023-01-01 10:05:00', 4000),
(5, 3, 35.1, '2023-01-01 10:00:00', 5000), (6, 3, 42.3, '2023-01-01 10:05:00', 6000);

INSERT INTO sensor_config VALUES 
(1, 20.0, 50.0, 1.05, 1000), (2, 15.0, 85.0, 0.98, 2000), (3, 30.0, 70.0, 1.02, 3000);

-- Join with mathematical expressions
SELECT 
  m.measure_id,
  m.reading,
  m.reading * sc.calibration_factor as calibrated_reading,
  sc.min_threshold,
  sc.max_threshold
FROM measurements m
INNER JOIN sensor_config sc ON m.sensor_id = sc.sensor_id
ORDER BY m.measure_id;

-- Join with conditional expressions
SELECT 
  m.measure_id,
  m.reading,
  sc.calibration_factor,
  CASE 
    WHEN m.reading * sc.calibration_factor < sc.min_threshold THEN 'Below Range'
    WHEN m.reading * sc.calibration_factor > sc.max_threshold THEN 'Above Range'
    ELSE 'In Range'
  END as reading_status
FROM measurements m
INNER JOIN sensor_config sc 
  ON m.sensor_id = sc.sensor_id 
  AND m.reading BETWEEN sc.min_threshold * 0.5 AND sc.max_threshold * 1.5
ORDER BY m.measure_id;

-- Join with aggregated expressions  
SELECT 
  sc.sensor_id,
  COUNT(*) as total_readings,
  AVG(m.reading * sc.calibration_factor) as avg_calibrated,
  COUNT(CASE WHEN m.reading * sc.calibration_factor > sc.max_threshold THEN 1 END) as over_threshold_count
FROM measurements m
INNER JOIN sensor_config sc ON m.sensor_id = sc.sensor_id
GROUP BY sc.sensor_id, sc.calibration_factor, sc.max_threshold
HAVING AVG(m.reading * sc.calibration_factor) > 30.0
ORDER BY avg_calibrated DESC;

-- Join with string expression conditions
CREATE TABLE devices(device_id INTEGER, device_code VARCHAR, "status" VARCHAR, ts TIMESTAMP TIME INDEX);
CREATE TABLE device_logs(log_id INTEGER, device_code VARCHAR, log_message VARCHAR, severity INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO devices VALUES 
(1, 'DEV001', 'active', 1000), (2, 'DEV002', 'inactive', 2000), (3, 'DEV003', 'active', 3000);

INSERT INTO device_logs VALUES 
(1, 'DEV001', 'System started', 1, 1000), (2, 'DEV001', 'Warning detected', 2, 2000),
(3, 'DEV002', 'Error occurred', 3, 3000), (4, 'DEV003', 'Normal operation', 1, 4000);

-- Join with string expression matching
SELECT 
  d.device_id,
  d."status",
  dl.log_message,
  dl.severity
FROM devices d
INNER JOIN device_logs dl 
  ON UPPER(d.device_code) = UPPER(dl.device_code)
  AND d."status" = 'active'
ORDER BY d.device_id, dl.severity DESC;

DROP TABLE measurements;

DROP TABLE sensor_config;

DROP TABLE devices;

DROP TABLE device_logs;