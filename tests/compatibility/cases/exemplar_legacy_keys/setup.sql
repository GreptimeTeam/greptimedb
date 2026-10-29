CREATE TABLE dense_keys (ts TIMESTAMP TIME INDEX, host STRING PRIMARY KEY, val DOUBLE);
INSERT INTO dense_keys (ts, host, val) VALUES (1, 'api', 1.5), (2, NULL, 2.5);
ADMIN FLUSH_TABLE('dense_keys');
CREATE TABLE metric_keys (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric
WITH ('physical_metric_table'='', 'memtable.type'='bulk', 'primary_key_encoding'='sparse');
CREATE TABLE logical_keys (ts TIMESTAMP TIME INDEX, host STRING PRIMARY KEY, val DOUBLE) ENGINE=metric
WITH ('on_physical_table'='metric_keys');
INSERT INTO logical_keys (ts, host, val) VALUES (1, 'api', 3.5), (2, NULL, 4.5);
ADMIN FLUSH_TABLE('metric_keys');
-- Historical OTLP infinity labels must retain their identity after upgrade.
CREATE TABLE otlp_upgrade_bucket (
    greptime_timestamp TIMESTAMP(3) TIME INDEX,
    le STRING PRIMARY KEY,
    greptime_value DOUBLE
);
INSERT INTO otlp_upgrade_bucket VALUES (10000, 'inf', 1), (20000, 'inf', 2);
ADMIN FLUSH_TABLE('otlp_upgrade_bucket');
