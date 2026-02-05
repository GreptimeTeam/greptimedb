-- SQLNESS ARG since=0.15.0
-- SQLNESS IGNORE_RESULT
CREATE TABLE granularity_and_false_positive_rate (
    ts timestamp time index, 
    val double
) with (
    "index.granularity" = "8192", 
    "index.false_positive_rate" = "0.01"
);
