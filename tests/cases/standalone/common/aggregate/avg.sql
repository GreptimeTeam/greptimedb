-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_avg.test
-- Test AVG aggregate function

-- scalar average
SELECT AVG(3), AVG(NULL);

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

SELECT AVG(DISTINCT i), AVG(DISTINCT j) FROM vals;

-- cleanup
DROP TABLE integers;

DROP TABLE vals;