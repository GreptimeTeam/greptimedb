-- Regression tests for PromQL execution plans that return mismatched
-- `Statistics::column_statistics` length.
--
-- DataFusion expects: `statistics.column_statistics.len() == schema.fields().len()`.
-- If not, `FilterExec` may fail while building `ExprBoundaries` with:
-- "Could not create `ExprBoundaries`: ... col_index has gone out of bounds ..."

-- -----------------------------------------------------------------------------
-- Case 1: HistogramFoldExec + topk(histogram_quantile(...)) on metric engine
-- -----------------------------------------------------------------------------

CREATE TABLE promql_stats_mismatch_physical (
  ts TIMESTAMP(3) TIME INDEX,
  val DOUBLE,
) ENGINE = metric WITH ("physical_metric_table" = "");

CREATE TABLE promql_stats_mismatch_hist_bucket (
  `cluster` STRING NULL,
  le STRING NULL,
  instance STRING NULL,
  operation STRING NULL,
  `type` STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  val DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(`cluster`, le, instance, operation, `type`),
)
ENGINE = metric
WITH(
  on_physical_table = 'promql_stats_mismatch_physical'
);

-- Counter samples at t=1ms and t=5m (300000ms). `rate(...[5m])` uses the delta in this window.
INSERT INTO promql_stats_mismatch_hist_bucket (`cluster`, le, instance, operation, `type`, ts, val) VALUES
  -- t = 1ms (avoid range-start boundary exclusion)
  ('cluster', '0.5',  'inst1', 'op', 't', 1, 0),
  ('cluster', '1',    'inst1', 'op', 't', 1, 0),
  ('cluster', '2',    'inst1', 'op', 't', 1, 0),
  ('cluster', '+Inf', 'inst1', 'op', 't', 1, 0),
  ('cluster', '0.5',  'inst2', 'op', 't', 1, 0),
  ('cluster', '1',    'inst2', 'op', 't', 1, 0),
  ('cluster', '2',    'inst2', 'op', 't', 1, 0),
  ('cluster', '+Inf', 'inst2', 'op', 't', 1, 0),
  ('cluster', '0.5',  'inst3', 'op', 't', 1, 0),
  ('cluster', '1',    'inst3', 'op', 't', 1, 0),
  ('cluster', '2',    'inst3', 'op', 't', 1, 0),
  ('cluster', '+Inf', 'inst3', 'op', 't', 1, 0),
  ('cluster', '0.5',  'inst4', 'op', 't', 1, 0),
  ('cluster', '1',    'inst4', 'op', 't', 1, 0),
  ('cluster', '2',    'inst4', 'op', 't', 1, 0),
  ('cluster', '+Inf', 'inst4', 'op', 't', 1, 0),
  ('cluster', '0.5',  'inst5', 'op', 't', 1, 0),
  ('cluster', '1',    'inst5', 'op', 't', 1, 0),
  ('cluster', '2',    'inst5', 'op', 't', 1, 0),
  ('cluster', '+Inf', 'inst5', 'op', 't', 1, 0),
  ('cluster', '0.5',  'inst6', 'op', 't', 1, 0),
  ('cluster', '1',    'inst6', 'op', 't', 1, 0),
  ('cluster', '2',    'inst6', 'op', 't', 1, 0),
  ('cluster', '+Inf', 'inst6', 'op', 't', 1, 0),
  -- t = 300000ms (5m)
  ('cluster', '0.5',  'inst1', 'op', 't', 300000, 95),
  ('cluster', '1',    'inst1', 'op', 't', 300000, 98),
  ('cluster', '2',    'inst1', 'op', 't', 300000, 100),
  ('cluster', '+Inf', 'inst1', 'op', 't', 300000, 100),
  ('cluster', '0.5',  'inst2', 'op', 't', 300000, 50),
  ('cluster', '1',    'inst2', 'op', 't', 300000, 95),
  ('cluster', '2',    'inst2', 'op', 't', 300000, 100),
  ('cluster', '+Inf', 'inst2', 'op', 't', 300000, 100),
  ('cluster', '0.5',  'inst3', 'op', 't', 300000, 50),
  ('cluster', '1',    'inst3', 'op', 't', 300000, 75),
  ('cluster', '2',    'inst3', 'op', 't', 300000, 100),
  ('cluster', '+Inf', 'inst3', 'op', 't', 300000, 100),
  ('cluster', '0.5',  'inst4', 'op', 't', 300000, 50),
  ('cluster', '1',    'inst4', 'op', 't', 300000, 80),
  ('cluster', '2',    'inst4', 'op', 't', 300000, 97),
  ('cluster', '+Inf', 'inst4', 'op', 't', 300000, 100),
  ('cluster', '0.5',  'inst5', 'op', 't', 300000, 10),
  ('cluster', '1',    'inst5', 'op', 't', 300000, 20),
  ('cluster', '2',    'inst5', 'op', 't', 300000, 100),
  ('cluster', '+Inf', 'inst5', 'op', 't', 300000, 100),
  ('cluster', '0.5',  'inst6', 'op', 't', 300000, 0),
  ('cluster', '1',    'inst6', 'op', 't', 300000, 0),
  ('cluster', '2',    'inst6', 'op', 't', 300000, 100),
  ('cluster', '+Inf', 'inst6', 'op', 't', 300000, 100);

-- This used to error due to HistogramFoldExec returning `column_statistics` with wrong length.
TQL EVAL (300, 300, '300s')
  topk(
    5,
    histogram_quantile(
      0.95,
      sum(rate(promql_stats_mismatch_hist_bucket{cluster="cluster"}[5m])) by (cluster, le, instance, operation, type)
    )
  ) AS q95;

DROP TABLE promql_stats_mismatch_hist_bucket;
DROP TABLE promql_stats_mismatch_physical;

-- -----------------------------------------------------------------------------
-- Case 2: InstantManipulateExec stats mismatch with nested Arrow fields
-- -----------------------------------------------------------------------------
--
-- Metric engine enforces float64 field + string tags, so it can't create a nested schema
-- needed to reproduce the `flattened_fields().len()` vs `fields().len()` mismatch.
-- Use a normal table with a structured JSON (Arrow Struct) field column instead.

CREATE TABLE promql_instant_mismatch_nested (
  ts TIMESTAMP(3) TIME INDEX,
  k STRING PRIMARY KEY,
  v JSON(format = "structured"),
);

INSERT INTO promql_instant_mismatch_nested VALUES
  (0, 'a', '{"x": 1}'),
  (1000, 'a', '{"x": 2}');

-- This used to error due to InstantManipulateExec returning `column_statistics` sized by
-- `schema.flattened_fields().len()` when the schema contains nested fields (Arrow Struct).
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 1, '1s') promql_instant_mismatch_nested == promql_instant_mismatch_nested;

DROP TABLE promql_instant_mismatch_nested;
