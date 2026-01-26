-- test if `ts` from appear in group by twice(and cause query failure)

CREATE TABLE phy_metric_min (
  ts timestamp(3) time index,
  tag_a STRING,
  v DOUBLE NULL,
  PRIMARY KEY (tag_a)
) ENGINE = metric WITH ("physical_metric_table" = "");

SHOW CREATE TABLE phy_metric_min;

CREATE TABLE IF NOT EXISTS metric_min (
  tag_a STRING,
  ts TIMESTAMP(3) NOT NULL,
  v DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY (tag_a)
) ENGINE=metric WITH(
  on_physical_table = 'phy_metric_min'
);

INSERT INTO metric_min
  (ts, v, tag_a)
VALUES
  ('2026-01-23T03:40:00Z', 1.0, 'alpha'),
  ('2026-01-23T03:41:00Z', 2.0, 'alpha'),
  ('2026-01-23T03:41:00Z', 4.0, 'beta');

-- Substrait encode/decode check via TQL pushdown on metric_min
-- test if `ts` from appear in group by twice(and cause query failure)
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE __table_id\s=\sUInt32\(\d+\) __table_id=UInt32(REDACTED)
TQL EXPLAIN (
  timestamp '2026-01-23 03:38:00+00',
  timestamp '2026-01-23 03:44:00+00',
  '1m'
)
sum by (tag_a, ts) (
  sum_over_time(metric_min{tag_a!=""}[2m])
);

-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE __table_id\s=\sUInt32\(\d+\) __table_id=UInt32(REDACTED)
TQL EXPLAIN (
  timestamp '2026-01-23 03:38:00+00',
  timestamp '2026-01-23 03:44:00+00',
  '1m'
)
sum_over_time(metric_min{tag_a!=""}[2m])
;

TQL EVAL (
  timestamp '2026-01-23 03:38:00+00',
  timestamp '2026-01-23 03:44:00+00',
  '1m'
)
sum by (tag_a, ts) (
  sum_over_time(metric_min{tag_a!=""}[2m])
);

DROP TABLE metric_min;
DROP TABLE phy_metric_min;
