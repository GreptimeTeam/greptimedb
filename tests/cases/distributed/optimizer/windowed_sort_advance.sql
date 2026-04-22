create table `a` (`value` double, `status` bigint, ts timestamp(9) time index);

INSERT INTO `a` (`value`, `status`, `ts`) VALUES
(46.82, 200, '2026-03-12T08:00:05.000000000+08:00'),
(46.84, 200, '2026-03-12T08:00:15.000000000+08:00'),
(46.85, 200, '2026-03-12T08:00:25.000000000+08:00'),
(46.86, 200, '2026-03-12T08:00:35.000000000+08:00'),
(46.88, 200, '2026-03-12T08:00:45.000000000+08:00'),
(46.89, 200, '2026-03-12T08:00:55.000000000+08:00'),
(46.91, 200, '2026-03-12T08:01:05.000000000+08:00'),
(46.90, 200, '2026-03-12T08:01:15.000000000+08:00'),
(46.87, 200, '2026-03-12T08:01:25.000000000+08:00'),
(46.85, 200, '2026-03-12T08:01:35.000000000+08:00');

select ts, status, value from `a` where ts >= '2026-03-12T08:00:00+08:00' and ts < '2026-03-12T08:02:01+08:00' order by ts asc;

select ts as ts, status, value from `a` where ts >= '2026-03-12T08:00:00+08:00' and ts < '2026-03-12T08:02:01+08:00' order by ts asc;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE select ts as ts, status, value from `a` where ts >= '2026-03-12T08:00:00+08:00' and ts < '2026-03-12T08:02:01+08:00' order by ts asc;

select to_timestamp_millis(ts) as ts, status, value from `a` where ts >= '2026-03-12T08:00:00+08:00' and ts < '2026-03-12T08:02:01+08:00' order by ts asc;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE select to_timestamp_millis(ts) as ts, status, value from `a` where ts >= '2026-03-12T08:00:00+08:00' and ts < '2026-03-12T08:02:01+08:00' order by ts asc;

DROP TABLE `a`;
