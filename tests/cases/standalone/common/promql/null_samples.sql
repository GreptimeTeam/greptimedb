CREATE TABLE null_samples (
    ts TIMESTAMP(3) TIME INDEX,
    "region" STRING,
    host STRING,
    val DOUBLE,
    PRIMARY KEY ("region", host)
);

INSERT INTO null_samples VALUES
    (0, 'empty', 'e', NULL),
    (1000, 'empty', 'e', NULL),
    (0, 'ok', 'o', 1.0),
    (1000, 'ok', 'o', 2.0),
    (2000, 'ok', 'o', 3.0);

-- The `empty` series has no sample in any window, so it must not reach the aggregation.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '1s') avg by (region) (rate(null_samples[4s]));

DROP TABLE null_samples;

CREATE TABLE sparse_samples (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE
);

INSERT INTO sparse_samples VALUES
    (0, 'a', 1.0),
    (1000, 'a', NULL),
    (2000, 'a', NULL),
    (3000, 'a', 4.0);

-- Every window below holds two samples, 1.0 at 0s and 4.0 at 3s.
TQL EVAL (3, 3, '1s') rate(sparse_samples[4s]);

TQL EVAL (3, 3, '1s') increase(sparse_samples[4s]);

TQL EVAL (3, 3, '1s') delta(sparse_samples[4s]);

TQL EVAL (3, 3, '1s') idelta(sparse_samples[4s]);

TQL EVAL (3, 3, '1s') irate(sparse_samples[4s]);

TQL EVAL (3, 3, '1s') changes(sparse_samples[4s]);

TQL EVAL (3, 3, '1s') resets(sparse_samples[4s]);

TQL EVAL (3, 3, '1s') avg_over_time(sparse_samples[4s]);

TQL EVAL (3, 3, '1s') stddev_over_time(sparse_samples[4s]);

TQL EVAL (3, 3, '1s') stdvar_over_time(sparse_samples[4s]);

TQL EVAL (3, 3, '1s') quantile_over_time(0.5, sparse_samples[4s]);

-- A window whose only slot is NULL holds no sample.
TQL EVAL (1, 1, '1s') rate(sparse_samples[1s]);

TQL EVAL (1, 1, '1s') changes(sparse_samples[1s]);

-- Prometheus returns an empty vector for a range without samples, not NaN.
TQL EVAL (1, 1, '1s') quantile_over_time(0.5, sparse_samples[1s]);

DROP TABLE sparse_samples;

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

-- f1 and f2 share the same rows but have samples at different timestamps. Dropping the rows
-- where f2 is NULL would also drop two of f1's samples, so each field is counted separately.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (3, 3, '1s') rate(multi_field[4s]);

-- f1 has two samples in this window and f2 only one, so f1 keeps its result while f2 is NULL.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') rate(multi_field[4s]);

-- A selector emits the same shape: the row stays, the field without a sample is NULL.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') multi_field;

DROP TABLE multi_field;
