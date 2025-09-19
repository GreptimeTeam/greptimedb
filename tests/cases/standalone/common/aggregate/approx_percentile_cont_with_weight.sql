-- Migrated from DuckDB test style: test weighted approximate percentile
-- Test APPROX_PERCENTILE_CONT_WITH_WEIGHT function

-- Test basic weighted percentile
CREATE TABLE weight_test("value" INTEGER, weight INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO weight_test VALUES
    (10, 1, 1000), (20, 2, 2000), (30, 3, 3000), (40, 4, 4000), (50, 1, 5000);

-- Test 50th percentile (median) with weights
SELECT approx_percentile_cont_with_weight(weight, 0.5) WITHIN GROUP (ORDER BY "value") FROM weight_test;

-- Test different percentiles
SELECT approx_percentile_cont_with_weight(weight, 0.25) WITHIN GROUP (ORDER BY "value") FROM weight_test;

SELECT approx_percentile_cont_with_weight(weight, 0.75) WITHIN GROUP (ORDER BY "value") FROM weight_test;

-- Test with groups
CREATE TABLE weight_groups(grp INTEGER, "value" INTEGER, weight INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO weight_groups VALUES
    (1, 10, 2, 1000), (1, 20, 3, 2000), (1, 30, 1, 3000),
    (2, 100, 1, 4000), (2, 200, 4, 5000), (2, 300, 2, 6000);

SELECT grp, approx_percentile_cont_with_weight(weight, 0.5) WITHIN GROUP (ORDER BY "value")
FROM weight_groups GROUP BY grp ORDER BY grp;

-- Test with double values and weights
CREATE TABLE weight_double("value" DOUBLE, "weight" DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO weight_double VALUES
    (1.5, 0.5, 1000), (2.5, 1.0, 2000), (3.5, 1.5, 3000), (4.5, 2.0, 4000);

SELECT approx_percentile_cont_with_weight("weight", 0.5) WITHIN GROUP (ORDER BY "value") FROM weight_double;

-- Test edge cases
-- min
SELECT approx_percentile_cont_with_weight("weight", 0.0) WITHIN GROUP (ORDER BY "value") FROM weight_test;

-- max
SELECT approx_percentile_cont_with_weight("weight", 1.0) WITHIN GROUP (ORDER BY "value") FROM weight_test;

-- Test with zero weights
CREATE TABLE zero_weight("value" INTEGER, weight INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO zero_weight VALUES
    (10, 0, 1000), (20, 1, 2000), (30, 0, 3000), (40, 2, 4000);

--TODO: this result is unstable currently
--SELECT approx_percentile_cont_with_weight(weight, 0.5) WITHIN GROUP (ORDER BY "value") FROM zero_weight;

-- Test with NULL values
INSERT INTO weight_test VALUES (NULL, 1, 6000), (60, NULL, 7000);

SELECT approx_percentile_cont_with_weight(weight, 0.5) WITHIN GROUP (ORDER BY "value") FROM weight_test;

-- Test empty result
SELECT approx_percentile_cont_with_weight(weight, 0.5) WITHIN GROUP (ORDER BY "value")
FROM weight_test WHERE "value" > 1000;

-- Test single weighted value
CREATE TABLE single_weight("value" INTEGER, weight INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO single_weight VALUES (42, 5, 1000);

SELECT approx_percentile_cont_with_weight(weight, 0.5) WITHIN GROUP (ORDER BY "value") FROM single_weight;

-- Test equal weights (should behave like regular percentile)
CREATE TABLE equal_weight("value" INTEGER, weight INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO equal_weight VALUES
    (10, 1, 1000), (20, 1, 2000), (30, 1, 3000), (40, 1, 4000);

SELECT
    approx_percentile_cont_with_weight(weight, 0.5) WITHIN GROUP (ORDER BY "value"),
    approx_percentile_cont(0.5) WITHIN GROUP (ORDER BY "value")
FROM equal_weight;

-- cleanup
DROP TABLE weight_test;

DROP TABLE weight_groups;

DROP TABLE weight_double;

DROP TABLE zero_weight;

DROP TABLE single_weight;

DROP TABLE equal_weight;
