SELECT ts, host, val FROM dense_keys ORDER BY ts;
SELECT ts, host, val FROM logical_keys ORDER BY ts;
INSERT INTO dense_keys (ts, host, val) VALUES (3, 'new', 5.5);
INSERT INTO logical_keys (ts, host, val) VALUES (3, 'new', 6.5);
ADMIN FLUSH_TABLE('dense_keys');
ADMIN FLUSH_TABLE('metric_keys');
SELECT COUNT(*) FROM dense_keys;
SELECT COUNT(*) FROM logical_keys;
-- New OTLP writes retain the historical infinity label across upgrade.
INSERT INTO otlp_upgrade_bucket VALUES (30000, 'inf', 3), (40000, 'inf', 4);
SELECT greptime_timestamp, le, greptime_value FROM otlp_upgrade_bucket ORDER BY greptime_timestamp;
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (10, 40, '10s') otlp_upgrade_bucket{le="inf"};
