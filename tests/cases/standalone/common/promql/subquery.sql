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
-- Reference: Prometheus `evaluator.subqueryTimeRange` (promql/engine.go) evaluates the inner
-- expression over `(start - offset - range, end - offset]`; `evalSubquery` then passes the
-- resulting samples, which keep their real timestamps, to the outer range-vector function as a
-- `MatrixSelector` that still carries the subquery offset. See also
-- promql/promqltest/testdata/subquery.test.
--
-- Every offset below is a whole multiple of the subquery step. Prometheus anchors subquery step
-- points on absolute epoch multiples of the step, while GreptimeDB anchors them on the
-- evaluation start, so the two only agree for step-multiple offsets; sub-step offsets are
-- deliberately not pinned here.
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

-- a zero subquery offset is not expressible: the shared duration check in `promql-parser`
-- rejects any zero duration literal, matching Prometheus's own `parseDuration`
-- ("duration must be greater than 0", cf. its `foo[0m]` parser test). The no-op case is
-- therefore covered by the offset-free baseline above.
tql eval (60, 60, '1s') sum_over_time(subquery_offset_total[20s:10s] offset 0s);

-- a negative offset looks ahead of the evaluation time
tql eval (30, 30, '1s') sum_over_time(subquery_offset_total[20s:10s] offset -30s);

-- an offset on the inner selector composes additively with the subquery offset: Prometheus
-- `subqueryTimes` accumulates "the sum of offsets and ranges of all subqueries in the path",
-- and the inner selector subtracts its own offset from the already shifted step timestamps.
-- 20s + 10s therefore behaves like the un-offset subquery at t=30.
tql eval (60, 60, '1s') sum_over_time((subquery_offset_total offset 10s)[20s:10s] offset 20s);

-- ... and the inner offset alone accounts for the same total shift
tql eval (60, 60, '1s') sum_over_time((subquery_offset_total offset 30s)[20s:10s]);

drop table subquery_offset_total;
