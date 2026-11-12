-- Tests for the PromQL `@` modifier on subqueries.
--
-- Prometheus folds the `@` of a subquery into its offset (`setOffsetForAtModifier`) and treats
-- the subquery as step-invariant: every evaluation step reports the window
-- `(anchor - offset - range, anchor - offset]`, sampled on the absolute multiples of the subquery
-- step, exactly like `@` on a range selector. `@ start()` / `@ end()` anchor at the start/end of
-- the statement's evaluation range. The output timestamps still follow the evaluation grid.
--
-- Two series with different values, so that a dropped or merged series shows up as a wrong row.
-- Sample timestamps are in milliseconds, while `@` and `TQL EVAL` timestamps are in seconds.
-- The samples lie on the 10s subquery step grid.

CREATE TABLE subquery_at_total (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE,
);

INSERT INTO subquery_at_total VALUES
    (0, 'a', 1),
    (10000, 'a', 2),
    (20000, 'a', 3),
    (30000, 'a', 4),
    (40000, 'a', 5),
    (50000, 'a', 6),
    (60000, 'a', 7),
    (0, 'b', 10),
    (10000, 'b', 20),
    (20000, 'b', 30),
    (30000, 'b', 40),
    (40000, 'b', 50),
    (50000, 'b', 60),
    (60000, 'b', 70);

-- The range selector form folds (40s, 60s] at every step: 6 + 7 = 13 (and 130 for 'b').
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') sum_over_time(subquery_at_total[20s] @ 60);

-- The subquery with `@ 60` must report the same window at every step.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') sum_over_time(subquery_at_total[20s:10s] @ 60);

-- Baseline without `@`: the window follows the grid, 3 + 4 at 30s and 6 + 7 at 60s.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') sum_over_time(subquery_at_total[20s:10s]);

-- `@ end()` anchors at 60s: 13 at both steps.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') sum_over_time(subquery_at_total[20s:10s] @ end());

-- `@ start()` anchors at 30s: (10s, 30s] holds 3 + 4 = 7 at both steps.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') sum_over_time(subquery_at_total[20s:10s] @ start());

-- `offset` applies relative to the anchor, in either modifier order: `@ 60 offset 30s` folds
-- (10s, 30s] = 7 at both steps.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') sum_over_time(subquery_at_total[20s:10s] @ 60 offset 30s);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') sum_over_time(subquery_at_total[20s:10s] offset 30s @ 60);

-- An instant query away from the anchor still folds the anchored window: 13.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') sum_over_time(subquery_at_total[20s:10s] @ 60);

-- Other `*_over_time` functions over the anchored window (40s, 60s]: points 50s and 60s.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') max_over_time(subquery_at_total[20s:10s] @ 60);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') count_over_time(subquery_at_total[20s:10s] @ 60);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') avg_over_time(subquery_at_total[20s:10s] @ 60);

-- `rate` over (30s, 60s] -- points 40s, 50s, 60s -- extrapolates from the anchored window, not
-- from the evaluation step, so the subquery form matches the range selector form at every step.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') rate(subquery_at_total[30s] @ 60);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') rate(subquery_at_total[30s:10s] @ 60);

-- A computed inner expression and an aggregation above the anchored call.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') sum_over_time((subquery_at_total * 2)[20s:10s] @ 60);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (30, 60, '30s') sum(sum_over_time(subquery_at_total[20s:10s] @ 60));

DROP TABLE subquery_at_total;
