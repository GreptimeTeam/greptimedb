-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_stddev.test
-- Test STDDEV aggregations

CREATE TABLE stddev_test(val INTEGER, grp INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO stddev_test VALUES (42, 1, 1000), (43, 1, 2000), (42, 2, 3000), (1000, 2, 4000), (NULL, 1, 5000), (NULL, 3, 6000);

SELECT stddev_samp(1);

SELECT var_samp(1);

-- stddev_samp
SELECT round(stddev_samp(val), 1) FROM stddev_test;

SELECT round(stddev_samp(val), 1) FROM stddev_test WHERE val IS NOT NULL;

SELECT grp, sum(val), round(stddev_samp(val), 1), min(val) FROM stddev_test GROUP BY grp ORDER BY grp;

SELECT grp, sum(val), round(stddev_samp(val), 1), min(val) FROM stddev_test WHERE val IS NOT NULL GROUP BY grp ORDER BY grp;

-- stddev_pop
SELECT round(stddev_pop(val), 1) FROM stddev_test;

SELECT round(stddev_pop(val), 1) FROM stddev_test WHERE val IS NOT NULL;

SELECT grp, sum(val), round(stddev_pop(val), 1), min(val) FROM stddev_test GROUP BY grp ORDER BY grp;

SELECT grp, sum(val), round(stddev_pop(val), 1), min(val) FROM stddev_test WHERE val IS NOT NULL GROUP BY grp ORDER BY grp;

-- var_samp
SELECT round(var_samp(val), 1) FROM stddev_test;

SELECT round(var_samp(val), 1) FROM stddev_test;

SELECT round(var_samp(val), 1) FROM stddev_test WHERE val IS NOT NULL;

SELECT grp, sum(val), round(var_samp(val), 1), min(val) FROM stddev_test GROUP BY grp ORDER BY grp;

SELECT grp, sum(val), round(var_samp(val), 1), min(val) FROM stddev_test WHERE val IS NOT NULL GROUP BY grp ORDER BY grp;

-- var_pop
SELECT round(var_pop(val), 1) FROM stddev_test;

SELECT round(var_pop(val), 1) FROM stddev_test WHERE val IS NOT NULL;

SELECT grp, sum(val), round(var_pop(val), 2), min(val) FROM stddev_test GROUP BY grp ORDER BY grp;

SELECT grp, sum(val), round(var_pop(val), 2), min(val) FROM stddev_test WHERE val IS NOT NULL GROUP BY grp ORDER BY grp;

-- cleanup
DROP TABLE stddev_test;
