-- SQLNESS VERSION version >= 0.15.0
CREATE TABLE granularity_and_false_positive_rate (
    ts timestamp time index, 
    val double
) with (
    "index.granularity" = "8192", 
    "index.false_positive_rate" = "0.01"
);

-- SQLNESS VERSION version >= 99.0.0
SELECT * FROM __sqlness_since_till_should_not_exist__;

-- SQLNESS VERSION version <= 0.1.0
SELECT * FROM __sqlness_since_till_should_not_exist__;

-- SQLNESS VERSION version >= 0.1.0 AND version <= 99.0.0
SELECT 1;
