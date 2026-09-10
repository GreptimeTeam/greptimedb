-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_avg.test
-- Test AVG aggregate function

-- scalar average
SELECT AVG(3);

-- FIXME(dennis): unsupported type
-- SELECT AVG(NULL);

SELECT AVG(3::SMALLINT), AVG(NULL::SMALLINT);

SELECT AVG(3::DOUBLE), AVG(NULL::DOUBLE);

-- test average with table
CREATE TABLE integers(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO integers VALUES (1, 1000), (2, 2000), (3, 3000);

SELECT AVG(i), AVG(1), AVG(DISTINCT i), AVG(NULL) FROM integers;

SELECT AVG(i) FROM integers WHERE i > 100;

-- empty average
CREATE TABLE vals(i INTEGER, j DOUBLE, k BIGINT, ts TIMESTAMP TIME INDEX);

INSERT INTO vals VALUES (NULL, NULL, NULL, 1000);

SELECT AVG(i), AVG(j), AVG(k) FROM vals;

-- test with mixed values
DROP TABLE vals;

CREATE TABLE vals(i INTEGER, j DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO vals VALUES (1, 1.5, 1000), (2, 2.5, 2000), (3, 3.5, 3000), (NULL, NULL, 4000);

SELECT AVG(i), AVG(j) FROM vals;

-- FIXME(dennis): AVG(DISTINCT) not supported
-- https://github.com/apache/datafusion/issues/2408
-- SELECT AVG(DISTINCT i), AVG(DISTINCT j) FROM vals;

-- AVG1 state scalar calculation
SELECT avg_calc(avg_state(CAST(i AS DOUBLE))) FROM integers;

-- Merged states retain their unequal group weights.
WITH states AS (
  SELECT avg_state(CAST(i AS DOUBLE)) AS state FROM integers WHERE i < 3
  UNION ALL
  SELECT avg_state(CAST(i AS DOUBLE)) AS state FROM integers WHERE i >= 3
)
SELECT avg_calc(avg_merge(state)) FROM states;

-- Empty, null-only, and null binary states calculate to NULL.
SELECT avg_calc(avg_state(CAST(i AS DOUBLE))) FROM integers WHERE i > 100;
SELECT avg_calc(avg_state(NULL::DOUBLE));
SELECT avg_calc(NULL::BINARY);

-- Invalid AVG1 state propagates an error.
SELECT avg_calc(X'00');

-- cleanup
DROP TABLE integers;

DROP TABLE vals;
