-- MySQL-compatible function alias tests

-- ucase -> upper
SELECT
    ucase('dataFusion') AS ucase_value,
    upper('dataFusion') AS upper_value;

-- lcase -> lower
SELECT
    lcase('DataFusion') AS lcase_value,
    lower('DataFusion') AS lower_value;

-- ceiling -> ceil
SELECT
    ceiling(1.2) AS ceiling_pos,
    ceil(1.2) AS ceil_pos,
    ceiling(-1.2) AS ceiling_neg,
    ceil(-1.2) AS ceil_neg;

-- mid -> substr
SELECT
    mid('datafusion', 5, 3) AS mid_value,
    substr('datafusion', 5, 3) AS substr_value;

-- rand -> random
-- NOTE: RAND([seed]) is supported by MySQL, but seed is not supported here.
-- This test only validates that rand() exists and returns values in [0, 1).
SELECT rand() >= 0.0 AND rand() < 1.0 AS rand_in_range;

-- std -> stddev_pop, variance -> var_pop
SELECT
    round(std(x), 6) AS std_value,
    round(stddev_pop(x), 6) AS stddev_pop_value,
    round(variance(x), 6) AS variance_value,
    round(var_pop(x), 6) AS var_pop_value
FROM (VALUES (1.0), (2.0), (3.0)) AS t(x);
