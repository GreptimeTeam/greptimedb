-- test if handle aliased sort expr correctly
CREATE TABLE IF NOT EXISTS lightning (
  collect_time TIMESTAMP(9) NOT NULL,
  collect_time_utc TIMESTAMP(9) NULL,
  peak_current FLOAT NULL,
  TIME INDEX (collect_time)
)
ENGINE=mito
WITH(
  'compaction.twcs.time_window' = '7d',
  'compaction.type' = 'twcs'
);

-- insert some data, with collect_time  = collect_time_utc + 8 hour
INSERT INTO lightning VALUES 
  ('2025-03-01 16:00:00', '2025-03-01 08:00:00', 1.0),
  ('2025-03-01 17:00:00', '2025-03-01 09:00:00', 1.0),
  ('2025-03-01 18:00:00', '2025-03-01 10:00:00', 1.0),
  ('2025-03-01 19:00:00', '2025-03-01 11:00:00', 1.0),
  ('2025-03-01 20:00:00', '2025-03-01 12:00:00', 1.0),
  ('2025-03-01 21:00:00', '2025-03-01 13:00:00', 1.0),
  ('2025-03-01 22:00:00', '2025-03-01 14:00:00', 1.0),
  ('2025-03-01 23:00:00', '2025-03-01 15:00:00', 1.0)
;

-- notice the alias make order by not applicable for window sort
-- note due to alias there is a tiny difference in the output between standalone/distributed
-- which is acceptable
SELECT
  collect_time_utc AS collect_time,
  peak_current,
FROM
  lightning
ORDER BY
  collect_time ASC;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT
  collect_time_utc AS collect_time,
  peak_current,
FROM
  lightning
ORDER BY
  collect_time ASC;

-- also try alias with different name with time index
SELECT
  collect_time_utc AS collect_time_0,
  peak_current,
FROM
  lightning
ORDER BY
  collect_time_0 ASC;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT
  collect_time_utc AS collect_time_0,
  peak_current,
FROM
  lightning
ORDER BY
  collect_time_0 ASC;

-- try more complex alias with time index
SELECT
  collect_time AS true_collect_time,
  collect_time_utc AS collect_time,
  peak_current,
FROM
  lightning
ORDER BY
  true_collect_time DESC;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE SELECT
  collect_time AS true_collect_time,
  collect_time_utc AS collect_time,
  peak_current,
FROM
  lightning
ORDER BY
  true_collect_time DESC;

-- this should also do windowed sort
SELECT
  collect_time_utc AS collect_time,
  collect_time AS true_collect_time,
  peak_current,
FROM
  lightning
ORDER BY
  true_collect_time DESC;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE SELECT
  collect_time_utc AS collect_time,
  collect_time AS true_collect_time,
  peak_current,
FROM
  lightning
ORDER BY
  true_collect_time DESC;

DROP TABLE lightning;

CREATE TABLE IF NOT EXISTS `instance_job_metrics` (
  `greptime_timestamp` TIMESTAMP(3) NOT NULL,
  `greptime_value` DOUBLE NULL,
  `instance` STRING NULL,
  `job` STRING NULL,
  TIME INDEX (`greptime_timestamp`),
  PRIMARY KEY (`instance`, `job`)
);

INSERT INTO `instance_job_metrics` VALUES
  ('2023-10-01 00:00:01.000', 1696118400.0, 'node1', 'job1'),
  ('2023-10-01 00:00:02.000', 1696118400.0, 'node2', 'job1'),
  ('2023-10-01 00:00:03.000', 1696118400.0, 'node3', 'job2');

TQL EVAL('2023-10-01 00:00:00.000'::TIMESTAMP, '2023-10-01 00:00:05.000'::TIMESTAMP, '1s') sum(instance_job_metrics);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE('2023-10-01 00:00:00.000'::TIMESTAMP, '2023-10-01 00:00:05.000'::TIMESTAMP, '1s') sum(instance_job_metrics);

DROP TABLE IF EXISTS `instance_job_metrics`;
