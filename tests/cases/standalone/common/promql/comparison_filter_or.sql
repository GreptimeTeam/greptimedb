-- Use metric engine to match real PromQL usage.
CREATE TABLE comparison_filter_or_physical (
    ts TIMESTAMP TIME INDEX,
    greptime_value DOUBLE
) ENGINE=metric WITH ("physical_metric_table" = "");

CREATE TABLE a (
    k STRING NULL,
    ts TIMESTAMP NOT NULL,
    greptime_value DOUBLE NULL,
    TIME INDEX (ts),
    PRIMARY KEY (k)
) ENGINE=metric WITH (on_physical_table = 'comparison_filter_or_physical');

CREATE TABLE b (
    k STRING NULL,
    ts TIMESTAMP NOT NULL,
    greptime_value DOUBLE NULL,
    TIME INDEX (ts),
    PRIMARY KEY (k)
) ENGINE=metric WITH (on_physical_table = 'comparison_filter_or_physical');

INSERT INTO a (ts, k, greptime_value)
VALUES
    (0, 'x', 3),
    (0, 'y', 1),
    (0, 'z', 5);

INSERT INTO b (ts, k, greptime_value)
VALUES
    (0, 'x', 1),
    (0, 'y', 2);

-- Regression: vector-vector comparison without `bool` is a filter that keeps LHS values.
-- It must not leak RHS columns (e.g. duplicated `ts`), otherwise set operators like `or`
-- can fail with ambiguous column references.
--
-- Also cover the common PromQL pattern `(A > B) or B or A`, which uses `or` to fill missing
-- series from RHS (e.g. B missing for some labels, then fallback to A).
-- SQLNESS SORT_RESULT 2 1
tql eval (0, 0, '1s') (a > b) or b or a;

-- Regression: derived vector expressions may not have a concrete `ctx.table_name`.
-- Function planning (which filters empty values) must not require a table name when the
-- input plan is valid.
-- SQLNESS SORT_RESULT 2 1
tql eval (0, 0, '1s') abs(a > b);

-- Regression: scalar-vector comparison (scalar on LHS) without `bool` is a filter that keeps
-- the vector side's samples/labels (not the scalar side).
CREATE TABLE m (
    k STRING NULL,
    ts TIMESTAMP NOT NULL,
    greptime_value DOUBLE NULL,
    TIME INDEX (ts),
    PRIMARY KEY (k)
) ENGINE=metric WITH (on_physical_table = 'comparison_filter_or_physical');

INSERT INTO m (ts, k, greptime_value)
VALUES
    (1000, 'x', 3),
    (1000, 'y', 0),
    (2000, 'x', 4),
    (2000, 'y', 0);

-- SQLNESS SORT_RESULT 3 1
tql eval (1, 2, '1s') time() < m;

drop table m;

drop table a;
drop table b;
drop table comparison_filter_or_physical;
