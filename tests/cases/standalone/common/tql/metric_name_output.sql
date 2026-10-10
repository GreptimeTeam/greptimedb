-- The PromQL metric-name identity is an internal column. A TQL statement is tabular, so the
-- identity must not be exposed in a direct TQL result, and a TQL CTE must expose the same
-- tabular schema, while the visible columns stay exactly as before.

CREATE TABLE metric_name_output (
  ts TIMESTAMP(3) TIME INDEX,
  host STRING PRIMARY KEY,
  val DOUBLE,
);

INSERT INTO metric_name_output VALUES
  (0, 'host1', 1.0),
  (10000, 'host2', 2.0),
  (20000, 'host1', 3.0);

-- Direct TQL: the selector keeps its visible columns only.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 20, '10s') metric_name_output;

-- A TQL CTE is tabular too: `SELECT *` exposes the same columns as the direct result.
-- SQLNESS SORT_RESULT 2 1
WITH tql AS (
  TQL EVAL (0, 20, '10s') metric_name_output
)
SELECT * FROM tql;

-- The CTE column arity check operates on the identity-free schema as well.
-- SQLNESS SORT_RESULT 2 1
WITH tql (the_ts, the_host, the_val) AS (
  TQL EVAL (0, 20, '10s') metric_name_output
)
SELECT * FROM tql;

DROP TABLE metric_name_output;
