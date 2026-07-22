CREATE TABLE legacy_dense_phy (
  ts TIMESTAMP TIME INDEX,
  val DOUBLE,
  host STRING,
  PRIMARY KEY (host)
) ENGINE = metric WITH ('physical_metric_table' = '');

CREATE TABLE legacy_dense_logical (
  ts TIMESTAMP TIME INDEX,
  val DOUBLE,
  host STRING,
  PRIMARY KEY (host)
) ENGINE = metric WITH ('on_physical_table' = 'legacy_dense_phy');

INSERT INTO legacy_dense_logical (ts, val, host) VALUES
  ('2026-01-01 00:00:00', 1.0, 'old-a'),
  ('2026-01-01 00:01:00', 2.0, 'old-b');

ADMIN FLUSH_TABLE('legacy_dense_phy');
