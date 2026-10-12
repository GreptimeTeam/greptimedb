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

-- Offset results match the shifted evaluation while retaining outer timestamps.
-- Verified against Prometheus v3.14.0.
tql eval (30, 60, '30s') sum_over_time(subquery_offset_total[20s:10s] offset 30s);

tql eval (0, 30, '30s') sum_over_time(subquery_offset_total[20s:10s]);

drop table subquery_offset_total;

-- Off-grid samples expose absolute step anchoring.
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

tql eval (30, 30, '1s') sum_over_time(subquery_offset_offgrid[20s:10s] offset 5s);

-- Offset moves the window without re-phasing its step points.
tql eval (30, 30, '1s') sum_over_time(subquery_offset_offgrid[20s:10s]);

-- The window starts before the first sample.
tql eval (30, 30, '1s') sum_over_time(subquery_offset_offgrid[20s:10s] offset 15s);

tql eval (15, 15, '1s') sum_over_time(subquery_offset_offgrid[20s:10s] offset -5s);

-- Multi-step evaluation uses the non-aligned parent timeline.
tql eval (35, 65, '30s') sum_over_time(subquery_offset_offgrid[20s:10s]);

-- An evaluation instant whose window reaches no sample has no row (30s), unlike 60s -> 2 + 3
tql eval (30, 60, '30s') sum_over_time(subquery_offset_offgrid[20s:10s] offset 30s);

-- No child point yields no row; the enclosing fallback still fires.
tql eval (35, 44, '10s') sum_over_time(subquery_offset_offgrid[5s:10s]);

tql eval (35, 44, '10s') sum_over_time(subquery_offset_offgrid[5s:10s]) or vector(1);

-- A negative offset makes the short window reach a child point.
tql eval (35, 44, '10s') sum_over_time(subquery_offset_offgrid[5s:10s] offset -5s);

-- An empty outer window suppresses inner results, but not an outer fallback.
tql eval (35, 44, '10s') sum_over_time((sum_over_time(subquery_offset_offgrid[5s:10s]))[5s:10s]);

tql eval (35, 44, '10s') sum_over_time((sum_over_time(subquery_offset_offgrid[5s:10s]))[5s:10s]) or vector(1);

-- An aligned instant has a child point in the same short range.
tql eval (30, 30, '1s') sum_over_time(subquery_offset_offgrid[5s:10s]);

-- A longer range covers child steps on the off-grid parent timeline.
tql eval (35, 65, '10s') sum_over_time(subquery_offset_offgrid[20s:10s]);

-- Zero step parses here but is invalid for planning.
tql eval (60, 60, '1s') sum_over_time(subquery_offset_offgrid[5s:0]);

tql eval (44, 35, '10s') sum_over_time(subquery_offset_offgrid[5s:10s]);

drop table subquery_offset_offgrid;
