-- Arithmetic between range functions that read the same selector and window.
-- Values must stay identical whether or not the planner shares one input.
CREATE TABLE shared_gauge (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE,
);

INSERT INTO shared_gauge VALUES
    (0, 'a', 1.0),
    (30000, 'a', 3.0),
    (60000, 'a', 5.0),
    (90000, 'a', 7.0),
    (0, 'b', 2.0),
    (30000, 'b', 2.0),
    (60000, 'b', 2.0),
    (90000, 'b', 8.0);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_gauge[1m]) + 2 * stddev_over_time(shared_gauge[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') min_over_time(shared_gauge[1m]) + max_over_time(shared_gauge[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') sum_over_time(shared_gauge[1m]) / count_over_time(shared_gauge[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') last_over_time(shared_gauge[1m]) - present_over_time(shared_gauge[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') stdvar_over_time(shared_gauge[1m]) + avg_over_time(shared_gauge[1m]);

-- The same call on both sides.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_gauge[1m]) + avg_over_time(shared_gauge[1m]);

-- Outer aggregation, unary and comparison over a shared input.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') sum(avg_over_time(shared_gauge[1m]) + 2 * stddev_over_time(shared_gauge[1m]));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') -(avg_over_time(shared_gauge[1m]) + stddev_over_time(shared_gauge[1m]));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_gauge[1m]) > stddev_over_time(shared_gauge[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_gauge[1m]) > bool stddev_over_time(shared_gauge[1m]);

-- Both operands carry the same offset.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_gauge[1m] offset 30s) + stddev_over_time(shared_gauge[1m] offset 30s);

-- Shapes that keep the original per-operand plan: different window, different offset,
-- explicit vector matching, a mixed instant operand, a two-argument function and a
-- different selector.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_gauge[1m]) + stddev_over_time(shared_gauge[2m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_gauge[1m]) + avg_over_time(shared_gauge[1m] offset 30s);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_gauge[1m]) + on(host) stddev_over_time(shared_gauge[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_gauge[1m]) + shared_gauge;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') quantile_over_time(0.5, shared_gauge[1m]) + avg_over_time(shared_gauge[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_gauge{host="a"}[1m]) + stddev_over_time(shared_gauge{host="b"}[1m]);

DROP TABLE shared_gauge;

-- Counter with a reset at 60s.
CREATE TABLE shared_counter (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE,
);

INSERT INTO shared_counter VALUES
    (0, 'a', 10.0),
    (30000, 'a', 20.0),
    (60000, 'a', 5.0),
    (90000, 'a', 15.0);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') rate(shared_counter[1m]) + increase(shared_counter[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') delta(shared_counter[1m]) + resets(shared_counter[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') idelta(shared_counter[1m]) + irate(shared_counter[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') changes(shared_counter[1m]) + deriv(shared_counter[1m]);

DROP TABLE shared_counter;

-- Isolated samples: a window holding one sample has no rate, so the product must
-- disappear instead of turning into a row without a sample.
CREATE TABLE shared_sparse (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE,
);

INSERT INTO shared_sparse VALUES
    (0, 'a', 1.0),
    (120000, 'a', 5.0),
    (150000, 'a', 9.0);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 180, '30s') count_over_time(shared_sparse[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 180, '30s') rate(shared_sparse[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 180, '30s') rate(shared_sparse[1m]) * count_over_time(shared_sparse[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 180, '30s') count_over_time(shared_sparse[1m]) + deriv(shared_sparse[1m]);

-- `rate` has no sample in a one-sample window, so raising it to 0 must not create one.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 180, '30s') (rate(shared_sparse[1m]) ^ 0) + count_over_time(shared_sparse[1m]);

DROP TABLE shared_sparse;

-- NULL fields are missing samples. An empty window must stay empty on both operands.
CREATE TABLE shared_null (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE,
);

INSERT INTO shared_null VALUES
    (0, 'a', 1.0),
    (30000, 'a', NULL),
    (60000, 'a', NULL),
    (90000, 'a', 4.0);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 120, '30s') count_over_time(shared_null[1m]) + avg_over_time(shared_null[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 120, '30s') rate(shared_null[1m]) + count_over_time(shared_null[1m]);

DROP TABLE shared_null;

-- Non-finite samples.
CREATE TABLE shared_nonfinite (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE,
);

INSERT INTO shared_nonfinite VALUES
    (0, 'a', 'NaN'::DOUBLE),
    (30000, 'a', 'Infinity'::DOUBLE),
    (60000, 'a', '-Infinity'::DOUBLE),
    (90000, 'a', 1.0);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') min_over_time(shared_nonfinite[1m]) + max_over_time(shared_nonfinite[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_nonfinite[1m]) + sum_over_time(shared_nonfinite[1m]);

DROP TABLE shared_nonfinite;

-- Two field columns keep the original per-operand plan.
CREATE TABLE shared_multi (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    v1 DOUBLE,
    v2 DOUBLE,
);

INSERT INTO shared_multi VALUES
    (0, 'a', 1.0, 10.0),
    (30000, 'a', 2.0, 20.0),
    (60000, 'a', 3.0, 30.0);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 60, '30s') avg_over_time(shared_multi[1m]) + max_over_time(shared_multi[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 60, '30s') avg_over_time(shared_multi{__field__="v1"}[1m]) + max_over_time(shared_multi{__field__="v1"}[1m]);

DROP TABLE shared_multi;

-- The same expressions after the data leaves the memtable.
CREATE TABLE shared_flushed (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE,
);

INSERT INTO shared_flushed VALUES
    (0, 'a', 10.0),
    (30000, 'a', 20.0),
    (60000, 'a', 5.0),
    (90000, 'a', 15.0);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_flushed[1m]) + 2 * stddev_over_time(shared_flushed[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') rate(shared_flushed[1m]) + increase(shared_flushed[1m]);

ADMIN FLUSH_TABLE('shared_flushed');

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') avg_over_time(shared_flushed[1m]) + 2 * stddev_over_time(shared_flushed[1m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 90, '30s') rate(shared_flushed[1m]) + increase(shared_flushed[1m]);

DROP TABLE shared_flushed;
