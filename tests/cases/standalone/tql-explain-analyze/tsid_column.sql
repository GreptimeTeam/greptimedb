CREATE TABLE tsid_physical (
  ts TIMESTAMP(3) TIME INDEX,
  val DOUBLE,
) ENGINE = metric WITH ("physical_metric_table" = "");

CREATE TABLE tsid_metric (
  job STRING NULL,
  instance STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  val DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(job, instance),
)
ENGINE = metric
WITH(
  on_physical_table = 'tsid_physical'
);

INSERT INTO tsid_metric VALUES
  ('job1', 'instance1', 0, 1),
  ('job1', 'instance2', 0, 2),
  ('job1', 'instance1', 5000, 3),
  ('job1', 'instance2', 5000, 4),
  ('job1', 'instance1', 10000, 5),
  ('job1', 'instance2', 10000, 6);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE (0, 10, '5s') sum(tsid_metric);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE (0, 10, '5s') sum by (job, instance) (tsid_metric);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE (0, 10, '5s')  sum(irate(tsid_metric[1h])) / scalar(count(count(tsid_metric) by (job)));

DROP TABLE tsid_metric;
DROP TABLE tsid_physical;

