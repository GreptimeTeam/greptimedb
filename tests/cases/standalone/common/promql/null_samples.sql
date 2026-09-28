-- NULL fields are missing samples, not zero-valued samples.
CREATE TABLE null_samples (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE,
);

INSERT INTO null_samples VALUES
    (0, 'a', 1.0),
    (1000, 'a', NULL),
    (2000, 'a', NULL),
    (3000, 'a', 4.0),
    (0, 'b', NULL),
    (1000, 'b', NULL),
    (2000, 'b', NULL),
    (3000, 'b', NULL);

-- At t=2 the trailing NULLs must not hide 1; at t=3 count must be 2.
-- At t=7 the left-open window is empty. Valid results must disappear.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (2, 7, '1s') count_over_time(null_samples{host="a"}[4s]);
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (2, 7, '1s') last_over_time(null_samples{host="a"}[4s]);
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (2, 7, '1s') present_over_time(null_samples{host="a"}[4s]);
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (2, 7, '1s') absent_over_time(null_samples{host="a"}[4s]);

-- All-NULL windows have no samples: only absent_over_time returns 1.
TQL EVAL (3, 3, '1s') count_over_time(null_samples{host="b"}[4s]);
TQL EVAL (3, 3, '1s') last_over_time(null_samples{host="b"}[4s]);
TQL EVAL (3, 3, '1s') present_over_time(null_samples{host="b"}[4s]);
TQL EVAL (3, 3, '1s') absent_over_time(null_samples{host="b"}[4s]);

-- Every function below sees the same two samples, 1.0 at 0s and 4.0 at 3s.
TQL EVAL (3, 3, '1s') rate(null_samples{host="a"}[4s]);

TQL EVAL (3, 3, '1s') increase(null_samples{host="a"}[4s]);

TQL EVAL (3, 3, '1s') delta(null_samples{host="a"}[4s]);

TQL EVAL (3, 3, '1s') idelta(null_samples{host="a"}[4s]);

TQL EVAL (3, 3, '1s') irate(null_samples{host="a"}[4s]);

TQL EVAL (3, 3, '1s') changes(null_samples{host="a"}[4s]);

TQL EVAL (3, 3, '1s') resets(null_samples{host="a"}[4s]);

TQL EVAL (3, 3, '1s') avg_over_time(null_samples{host="a"}[4s]);

TQL EVAL (3, 3, '1s') stddev_over_time(null_samples{host="a"}[4s]);

TQL EVAL (3, 3, '1s') stdvar_over_time(null_samples{host="a"}[4s]);

TQL EVAL (3, 3, '1s') quantile_over_time(0.5, null_samples{host="a"}[4s]);

-- A window whose only slot is NULL holds no sample, like a window with no row.
TQL EVAL (1, 1, '1s') rate(null_samples{host="a"}[1s]);

-- Prometheus returns an empty vector for a range without samples, not NaN.
TQL EVAL (1, 1, '1s') quantile_over_time(0.5, null_samples{host="a"}[1s]);

-- `b` never has a sample, so it must not reach the aggregation.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '1s') avg by (host) (rate(null_samples[4s]));

DROP TABLE null_samples;

CREATE TABLE multi_field (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    f1 DOUBLE,
    f2 DOUBLE
);

INSERT INTO multi_field VALUES
    (0, 'a', 1.0, 10.0),
    (1000, 'a', 2.0, NULL),
    (2000, 'a', 3.0, NULL),
    (3000, 'a', 4.0, 40.0);

-- f1 has two samples in this window and f2 only one, so f1 keeps its result while f2 is NULL.
-- Dropping the whole row would take f1's samples with it.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') rate(multi_field[4s]);

-- A selector emits the same shape: the row stays, the field without a sample is NULL.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') multi_field;

DROP TABLE multi_field;
