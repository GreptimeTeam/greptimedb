-- Regression test for `__tsid` optimization on non-aggregating PromQL queries.
-- The `__tsid` column must not affect operators that infer label columns from the input schema,
-- such as `histogram_quantile` (HistogramFold).

CREATE TABLE tsid_no_aggr_physical (
  ts TIMESTAMP(3) TIME INDEX,
  val DOUBLE,
) ENGINE = metric WITH ("physical_metric_table" = "");

CREATE TABLE tsid_no_aggr_metric (
  job STRING NULL,
  instance STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  val DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(job, instance),
)
ENGINE = metric
WITH(
  on_physical_table = 'tsid_no_aggr_physical'
);

CREATE TABLE tsid_no_aggr_histogram_bucket (
  job STRING NULL,
  le STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  val DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(job, le),
)
ENGINE = metric
WITH(
  on_physical_table = 'tsid_no_aggr_physical'
);

INSERT INTO tsid_no_aggr_metric VALUES
  ('job1', 'instance1', 0, 1),
  ('job1', 'instance1', 5000, 3),
  ('job1', 'instance1', 10000, 5);

INSERT INTO tsid_no_aggr_histogram_bucket VALUES
  ('job1', '1', 0, 1),
  ('job1', '2', 0, 2),
  ('job1', '+Inf', 0, 3),
  ('job1', '1', 5000, 2),
  ('job1', '2', 5000, 4),
  ('job1', '+Inf', 5000, 6),
  ('job1', '1', 10000, 3),
  ('job1', '2', 10000, 6),
  ('job1', '+Inf', 10000, 9);

-- Selector (no series merge)
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') tsid_no_aggr_metric;

-- Scalar function (no series merge)
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') abs(tsid_no_aggr_metric);

-- Range function (no series merge)
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') avg_over_time(tsid_no_aggr_metric[5s]);

-- Histogram quantile must fold buckets across `le` even when `__tsid` exists.
-- The physical plan must not treat `__tsid` as a label column for HistogramFold.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE (0, 10, '5s') histogram_quantile(0.5, tsid_no_aggr_histogram_bucket);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') histogram_quantile(0.5, tsid_no_aggr_histogram_bucket);

DROP TABLE tsid_no_aggr_histogram_bucket;
DROP TABLE tsid_no_aggr_metric;
DROP TABLE tsid_no_aggr_physical;

