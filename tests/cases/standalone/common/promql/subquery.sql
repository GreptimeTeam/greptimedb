create table metric_total (
    ts timestamp time index,
    val double,
);

insert into metric_total values
    (0, 1),
    (10000, 2);

tql eval (10, 10, '1s') sum_over_time(metric_total[50s:10s]);

tql eval (10, 10, '1s') sum_over_time(metric_total[50s:5s]);

tql eval (300, 300, '1s') sum_over_time(metric_total[50s:10s]);

tql eval (359, 359, '1s') sum_over_time(metric_total[60s:10s]);

tql eval (10, 10, '1s') rate(metric_total[20s:10s]);

tql eval (20, 20, '1s') rate(metric_total[20s:5s]);

drop table metric_total;

-- Offset on a subquery shifts the subquery's own evaluation window back by the offset.
-- Reference: Prometheus `evaluator.subqueryTimeRange` (promql/engine.go).
-- The offset cases stay on the subquery step grid so they match Prometheus directly.
create table subquery_offset_total (
    ts timestamp time index,
    host string primary key,
    val double,
);

insert into subquery_offset_total values
    (0, 'a', 1),
    (10000, 'a', 2),
    (20000, 'a', 3),
    (30000, 'a', 4),
    (40000, 'a', 5),
    (50000, 'a', 6),
    (60000, 'a', 7);

-- baseline: no offset at t=60 covers the 10s subquery points in (40s, 60s] -> 6 + 7
tql eval (60, 60, '1s') sum_over_time(subquery_offset_total[20s:10s]);

-- the same subquery evaluated at t=30 -> 3 + 4
tql eval (30, 30, '1s') sum_over_time(subquery_offset_total[20s:10s]);

-- `offset 30s` at t=60 must equal the un-offset subquery at t=30
tql eval (60, 60, '1s') sum_over_time(subquery_offset_total[20s:10s] offset 30s);

-- a negative offset looks ahead of the evaluation time
tql eval (30, 30, '1s') sum_over_time(subquery_offset_total[20s:10s] offset -30s);

-- an offset on the inner selector composes additively with the subquery offset: Prometheus
-- `subqueryTimes` accumulates "the sum of offsets and ranges of all subqueries in the path",
-- and the inner selector subtracts its own offset from the already shifted step timestamps.
-- 20s + 10s therefore behaves like the un-offset subquery at t=30.
tql eval (60, 60, '1s') sum_over_time((subquery_offset_total offset 10s)[20s:10s] offset 20s);

-- ... and the inner offset alone accounts for the same total shift
tql eval (60, 60, '1s') sum_over_time((subquery_offset_total offset 30s)[20s:10s]);

-- `predict_linear` predicts from the evaluation time: 4 + 0.1 * 30 = 7 and 7 - 0.1 * 30 = 4
tql eval (60, 60, '1s') predict_linear(subquery_offset_total[20s:10s] offset 30s, 0);

tql eval (30, 30, '1s') predict_linear(subquery_offset_total[20s:10s] offset -30s, 0);

-- A multi-step grid at a non-zero start, with an explicit subquery step: the offset shifts the
-- inner evaluation window back by 30s while the outer result keeps the timestamps of the grid it
-- was planned on. The offset query at 30s/60s therefore repeats the value of the un-offset
-- subquery at 0s/30s -- `(30s, 1)` and `(60s, 7)` against `(0s, 1)` and `(30s, 7)` -- and not the
-- values of its own grid, which would be 7 and 13.
--
-- Both rows and timestamps were verified against Prometheus v3.14.0 (`subqueryTimeRange`,
-- promql/engine.go) over the same samples: samples 0s..60s with values 1..7, subquery step 10s.
tql eval (30, 60, '30s') sum_over_time(subquery_offset_total[20s:10s] offset 30s);

tql eval (0, 30, '30s') sum_over_time(subquery_offset_total[20s:10s]);

drop table subquery_offset_total;

-- Prometheus anchors the inner evaluation points of a subquery on the absolute multiples of the
-- subquery step (`subqueryTimeRange`, promql/engine.go), not on the evaluation start, so an
-- offset moves the whole window without re-phasing its points. The samples below are 5s off that
-- step grid, which is what makes the anchoring observable: every expectation is from Prometheus
-- v3.14.0 over the same samples.
create table subquery_offset_offgrid (
    ts timestamp time index,
    host string primary key,
    val double,
);

insert into subquery_offset_offgrid values
    (5000, 'a', 1),
    (15000, 'a', 2),
    (25000, 'a', 3),
    (35000, 'a', 4),
    (45000, 'a', 5),
    (55000, 'a', 6);

-- `offset 5s` at 30s folds the inner points 10s and 20s of the window (5s, 25s] -> 1 + 2
tql eval (30, 30, '1s') sum_over_time(subquery_offset_offgrid[20s:10s] offset 5s);

-- ... while the un-offset window (10s, 30s] folds the points 20s and 30s -> 2 + 3, so the offset
-- case is not the un-offset case with a re-anchored grid
tql eval (30, 30, '1s') sum_over_time(subquery_offset_offgrid[20s:10s]);

-- `offset 15s` at 30s: the window (-5s, 15s] only reaches the sample at 5s -> 1
tql eval (30, 30, '1s') sum_over_time(subquery_offset_offgrid[20s:10s] offset 15s);

-- a negative offset looks ahead: the window (0s, 20s] at 15s folds the points 10s and 20s -> 1 + 2
tql eval (15, 15, '1s') sum_over_time(subquery_offset_offgrid[20s:10s] offset -5s);

-- a multi-step grid at a non-multiple start: (15s, 35s] -> 2 + 3 at 35s and (45s, 65s] -> 5 + 6 at
-- 65s, reported at the evaluation instants of that grid
tql eval (35, 65, '30s') sum_over_time(subquery_offset_offgrid[20s:10s]);

-- an evaluation instant whose window reaches no sample has no row (30s), unlike 60s -> 2 + 3
tql eval (30, 60, '30s') sum_over_time(subquery_offset_offgrid[20s:10s] offset 30s);

-- A range shorter than the subquery step leaves the child grid without a step when its first
-- multiple of the step lies past the parent's last step (35s here, the aligned end of 35s..44s).
-- Prometheus evaluates no child point then, so the subquery reports nothing -- it does not reject
-- the range -- and an enclosing `or` fallback still fires at the evaluation grid.
tql eval (35, 44, '10s') sum_over_time(subquery_offset_offgrid[5s:10s]);

tql eval (35, 44, '10s') sum_over_time(subquery_offset_offgrid[5s:10s]) or vector(1);

-- the child window of `offset -5s` at 35s is `(30s, 35s]` at the point 40s, i.e. the sample at 35s
tql eval (35, 44, '10s') sum_over_time(subquery_offset_offgrid[5s:10s] offset -5s);

-- the same is true one subquery deeper: the inner subquery has no child step either, so the outer
-- query reports nothing and its own fallback fires
tql eval (35, 44, '10s') sum_over_time((sum_over_time(subquery_offset_offgrid[5s:10s]))[5s:10s]);

tql eval (35, 44, '10s') sum_over_time((sum_over_time(subquery_offset_offgrid[5s:10s]))[5s:10s]) or vector(1);

-- an aligned evaluation instant with such a range does have a step: the child window is (25s, 30s]
-- folded at the point 30s, i.e. the sample at 25s
tql eval (30, 30, '1s') sum_over_time(subquery_offset_offgrid[5s:10s]);

-- the child grid of a range that does cover steps, on a parent grid whose end is not aligned
tql eval (35, 65, '10s') sum_over_time(subquery_offset_offgrid[20s:10s]);

-- a step of zero has no grid to divide by (`[5s:0]` parses, `[5s:0s]` is rejected as a duration)
tql eval (60, 60, '1s') sum_over_time(subquery_offset_offgrid[5s:0]);

-- ... and an inverted evaluation window is still rejected as an invalid range
tql eval (44, 35, '10s') sum_over_time(subquery_offset_offgrid[5s:10s]);

drop table subquery_offset_offgrid;
