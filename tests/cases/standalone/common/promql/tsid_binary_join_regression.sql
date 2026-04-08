-- Regression test for TSID-backed PromQL binary joins on metric-engine tables.
-- Default arithmetic and comparison joins should use `__tsid` when matching is the
-- default one-to-one case. Label modifiers still have to stay label-based.

CREATE TABLE tsid_binary_join_physical (
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
) ENGINE = metric WITH ("physical_metric_table" = "");

CREATE TABLE tsid_binary_join_left (
  host STRING NULL,
  job STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(host, job),
)
ENGINE = metric
WITH(
  on_physical_table = 'tsid_binary_join_physical'
);

CREATE TABLE tsid_binary_join_right (
  host STRING NULL,
  job STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(host, job),
)
ENGINE = metric
WITH(
  on_physical_table = 'tsid_binary_join_physical'
);

INSERT INTO tsid_binary_join_left (host, job, ts, greptime_value) VALUES
  ('host1', 'job1', 0, 12),
  ('host2', 'job2', 0, 18),
  ('host1', 'job1', 5000, 15),
  ('host2', 'job2', 5000, 21);

INSERT INTO tsid_binary_join_right (host, job, ts, greptime_value) VALUES
  ('host1', 'job1', 0, 3),
  ('host2', 'job2', 0, 6),
  ('host1', 'job1', 5000, 5),
  ('host2', 'job2', 5000, 7);

-- Default vector-vector arithmetic should join on `__tsid` and time index.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
TQL ANALYZE (0, 5, '5s') tsid_binary_join_left / tsid_binary_join_right;

-- Label modifiers must disable the TSID shortcut and keep matching on the remaining labels.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
TQL ANALYZE (0, 5, '5s') tsid_binary_join_left / ignoring(host) tsid_binary_join_right;

-- Comparison filters can join on `__tsid`, but the filtered result must still behave like
-- a regular derived vector downstream.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
TQL ANALYZE (0, 5, '5s') tsid_binary_join_left > tsid_binary_join_right;

-- `bool` comparison should follow the same TSID-backed matching path.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
TQL ANALYZE (0, 5, '5s') tsid_binary_join_left > bool tsid_binary_join_right;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 5, '5s') tsid_binary_join_left / tsid_binary_join_right;

DROP TABLE tsid_binary_join_right;
DROP TABLE tsid_binary_join_left;
DROP TABLE tsid_binary_join_physical;
