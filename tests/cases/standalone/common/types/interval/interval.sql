-- common test
SELECT INTERVAL '1 year 2 months 3 days 4 hours 5 minutes 6 seconds 100 microseconds';

SELECT INTERVAL '1.5 year';

SELECT INTERVAL '-2 months';

SELECT INTERVAL '1 year 2 months 3 days 4 hours' + INTERVAL '1 year';

SELECT INTERVAL '1 year 2 months 3 days 4 hours' - INTERVAL '1 year';

SELECT INTERVAL '6 years' * 2;

SELECT INTERVAL '6 years' / 2;

SELECT INTERVAL '6 years' = INTERVAL '72 months';

SELECT arrow_typeof(INTERVAL '1 month');

-- INTERVAL + TIME CONSTANT
SELECT current_time() + INTERVAL '1 hour';

-- table with interval type test
CREATE TABLE IF NOT EXISTS intervals(
  ts TIMESTAMP TIME INDEX,
  interval_value INTERVAL,
);

DESCRIBE TABLE intervals;

INSERT INTO intervals(ts, interval_value)
values
('2022-01-01 00:00:01', INTERVAL '1 year'),
('2022-01-01 00:00:02', INTERVAL '1 year'),
('2022-02-01 00:00:01', INTERVAL '2 year 2 months'),
('2022-03-01 00:00:01', INTERVAL '3 year 3 hours'),
('2022-04-01 00:00:01', INTERVAL '4 year 4 minutes'),
('2022-05-01 00:00:01', INTERVAL '5 year 5 seconds'),
('2022-06-01 00:00:01', INTERVAL '6 year 6 milliseconds'),
('2022-07-01 00:00:01', INTERVAL '7 year 7 microseconds'),
('2022-08-01 00:00:01', INTERVAL '8 year 8 nanoseconds'),
('2022-09-01 00:00:01', INTERVAL '9 year 9 days'),
('2022-10-01 00:00:01', INTERVAL '10 year 10 hours 10 minutes 10 seconds 10 milliseconds 10 microseconds 10 nanoseconds'),
('2022-11-01 00:00:01', INTERVAL '11 year 11 days 11 hours 11 minutes 11 seconds 11 milliseconds 11 microseconds 11 nanoseconds'),
('2022-12-01 00:00:01', INTERVAL '12 year 12 days 12 hours 12 minutes 12 seconds 12 milliseconds 12 microseconds 12 nanoseconds');

SELECT * FROM intervals;

SELECT DISTINCT interval_value FROM intervals ORDER BY interval_value;

-- ts + interval
SELECT ts + interval_value as new_value from intervals;

-- ts - interval
SELECT ts - interval_value as new_value from intervals;

-- DATE + INTERVAL
SELECT DATE '2000-10-30' + interval_value from intervals;

-- DATE - INTERVAL
SELECT DATE '2000-10-30' - interval_value from intervals;

-- INTERVAL + TIMESTAMP CONSTANT
SELECT TIMESTAMP '1992-09-20 11:30:00.123456' + interval_value as new_value from intervals;

-- TIMESTAMP CONSTANT - INTERVAL
SELECT TIMESTAMP '1992-09-20 11:30:00.123456' - interval_value as new_value from intervals;


-- Interval type does not support aggregation functions.
SELECT MIN(interval_value) from intervals;

SELECT MAX(interval_value) from intervals;

SELECT SUM(interval_value) from intervals;

SELECT AVG(interval_value) from intervals;

DROP TABLE intervals;
