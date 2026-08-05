-- Test: Sparse samples with intermediate empty windows and a valid trailing sample.
--
-- Two samples at t=0ms and t=100000ms (100s). A range vector with 25s window
-- should produce results at t=0s (the first sample) and t=120s (the trailing
-- sample), with the three intermediate steps returning no data (empty).

CREATE TABLE range_sparse_empty_trailing (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
);

INSERT INTO range_sparse_empty_trailing (ts, val) VALUES
    (0, 1.0),
    (100000, 1.0);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 120, '30s') count_over_time(range_sparse_empty_trailing[25s]);

DROP TABLE range_sparse_empty_trailing;
