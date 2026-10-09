-- Instant scalar parameters use @ timestamps in seconds; INSERT timestamps below are milliseconds.
CREATE TABLE dynamic_scalar_input (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    k STRING,
    PRIMARY KEY(k)
);

CREATE TABLE dynamic_scalar_bound (
    param_ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    p STRING,
    PRIMARY KEY(p)
);

INSERT INTO dynamic_scalar_input VALUES
    (0, 1.0, 'a'), (1000, 3.0, 'a'),
    (0, 4.0, 'b'), (1000, 7.0, 'b');

INSERT INTO dynamic_scalar_bound VALUES
    (0, 5.0, 'q'), (1000, 9.0, 'q'),
    (0, 6.0, 'r'), (1000, 10.0, 'r');

-- The scalar control reads 5 at @0.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') scalar(dynamic_scalar_bound{p="q"} @ 0);

-- Only the outer series labels remain, and output uses evaluation timestamp 1s.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') clamp_min(dynamic_scalar_input, scalar(dynamic_scalar_bound{p="q"} @ 0));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') clamp_max(dynamic_scalar_input, scalar(dynamic_scalar_bound{p="q"} @ 0));

-- Literal bounds control the equivalent clamp behavior.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') clamp_min(dynamic_scalar_input, 5);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') clamp_max(dynamic_scalar_input, 5);

-- Empty and multi-series scalar selectors produce NaN, not an empty output vector.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') clamp_min(dynamic_scalar_input, scalar(dynamic_scalar_bound{p="missing"} @ 0));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') clamp_min(dynamic_scalar_input, scalar(dynamic_scalar_bound @ 0));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') clamp_max(dynamic_scalar_input, scalar(dynamic_scalar_bound{p="missing"} @ 0));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') clamp_max(dynamic_scalar_input, scalar(dynamic_scalar_bound @ 0));

-- NaN quantile parameters retain both outer series.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') quantile_over_time(scalar(dynamic_scalar_bound{p="missing"} @ 0), dynamic_scalar_input[2s]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') quantile_over_time(scalar(dynamic_scalar_bound @ 0), dynamic_scalar_input[2s]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') quantile_over_time(scalar(vector(0.5)), dynamic_scalar_input[2s]);

-- The static quantile control uses the same full window and output timestamp.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') quantile_over_time(0.5, dynamic_scalar_input[2s]);

-- Anchored and offset windows keep the outer evaluation timestamp.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') quantile_over_time(scalar(vector(0.5)), dynamic_scalar_input[2s] @ 0);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') quantile_over_time(scalar(vector(0.5)), dynamic_scalar_input[2s] offset 1s);

DROP TABLE dynamic_scalar_bound;
DROP TABLE dynamic_scalar_input;
