create table `a_ms` (`value` double, `status` bigint, ts timestamp(3) time index);

INSERT INTO `a_ms` (`value`, `status`, `ts`) VALUES
(46.82, 200, '2026-03-12T08:00:05.123+08:00'),
(46.84, 200, '2026-03-12T08:00:15.234+08:00'),
(46.85, 200, '2026-03-12T08:00:25.345+08:00'),
(46.86, 200, '2026-03-12T08:00:35.456+08:00'),
(46.88, 200, '2026-03-12T08:00:45.567+08:00'),
(46.89, 200, '2026-03-12T08:00:55.678+08:00'),
(46.91, 200, '2026-03-12T08:01:05.789+08:00'),
(46.90, 200, '2026-03-12T08:01:15.890+08:00'),
(46.87, 200, '2026-03-12T08:01:25.901+08:00'),
(46.85, 200, '2026-03-12T08:01:35.999+08:00');

select ts, status, value from `a_ms` where ts >= '2026-03-12T08:00:00.000+08:00' and ts < '2026-03-12T08:02:01.000+08:00' order by ts asc;

select to_timestamp_nanos(ts) as ts, status, value from `a_ms` where ts >= '2026-03-12T08:00:00.000+08:00' and ts < '2026-03-12T08:02:01.000+08:00' order by ts asc;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE select to_timestamp_nanos(ts) as ts, status, value from `a_ms` where ts >= '2026-03-12T08:00:00.000+08:00' and ts < '2026-03-12T08:02:01.000+08:00' order by ts asc;

DROP TABLE `a_ms`;
