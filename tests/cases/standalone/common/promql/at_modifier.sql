-- Tests for the PromQL `@` modifier on vector and matrix selectors.
--
-- `@` anchors the sample selection window at a fixed timestamp instead of the timestamp of each
-- evaluation step: `@ <unix_ts>` uses the given timestamp, `@ start()` / `@ end()` use the
-- start/end of the statement's evaluation range, and `offset` shifts the anchor backwards before
-- the window is selected. The output timestamps still follow the evaluation grid.
--
-- Every metric below carries two series, host 'a' and host 'b', with different values: the
-- anchored results must therefore report both of them, per step. A series silently dropped, or a
-- batch holding several series handled as one timeline, does not look like a correct single-series
-- answer here but shows up as a missing or shifted row.
--
-- Sample timestamps below are in milliseconds, while `@` and `TQL EVAL` timestamps are in seconds,
-- as in Prometheus.

CREATE TABLE at_modifier_gauge (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    host STRING,
    PRIMARY KEY(host)
);

CREATE TABLE at_modifier_counter_total (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    host STRING,
    PRIMARY KEY(host)
);

-- One sample per minute, so the anchored sample is easy to identify. The default lookback is 5m.
-- Host 'a' reports 0.0 to 6.0, host 'b' reports 7.0 to 13.0: the two series are told apart by their
-- values alone, which stays true after any anchoring.
INSERT INTO at_modifier_gauge VALUES
    (0, 0.0, 'a'),
    (60000, 1.0, 'a'),
    (120000, 2.0, 'a'),
    (180000, 3.0, 'a'),
    (240000, 4.0, 'a'),
    (300000, 5.0, 'a'),
    (360000, 6.0, 'a'),
    (0, 7.0, 'b'),
    (60000, 8.0, 'b'),
    (120000, 9.0, 'b'),
    (180000, 10.0, 'b'),
    (240000, 11.0, 'b'),
    (300000, 12.0, 'b'),
    (360000, 13.0, 'b');

-- Host 'a': a counter increasing by 60 per minute, i.e. by 1 per second.
-- Host 'b': a counter starting at 100 and increasing by 30 per minute, i.e. by 0.5 per second. Its
-- rate differs from host 'a', so a window folded for the wrong series cannot pass as the right one.
INSERT INTO at_modifier_counter_total VALUES
    (0, 0.0, 'a'),
    (60000, 60.0, 'a'),
    (120000, 120.0, 'a'),
    (180000, 180.0, 'a'),
    (240000, 240.0, 'a'),
    (300000, 300.0, 'a'),
    (360000, 360.0, 'a'),
    (0, 100.0, 'b'),
    (60000, 130.0, 'b'),
    (120000, 160.0, 'b'),
    (180000, 190.0, 'b'),
    (240000, 220.0, 'b'),
    (300000, 250.0, 'b'),
    (360000, 280.0, 'b');

-- 1. Instant query anchored at an absolute timestamp: the sample window is `(anchor - lookback,
-- anchor]` = `(0s, 300s]`, so its newest samples are 5.0 for host 'a' and 12.0 for host 'b', both
-- at 300s. The output timestamps stay the evaluation timestamps.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 100, '1s') at_modifier_gauge @ 300;

-- Control: the same query without `@` looks back from the evaluation timestamp instead:
-- `(-200s, 100s]` selects the samples 1.0 (a) and 8.0 (b) at 60s.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 100, '1s') at_modifier_gauge;

-- The anchor, not the evaluation timestamp, decides which sample is reported.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 100, '1s') at_modifier_gauge @ 360;

-- Sub-second precision is preserved in the anchor: `240.999` selects the samples at 240s.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 100, '1s') at_modifier_gauge @ 240.999;

-- The window `(-300s, 0s]` includes the samples exactly at the anchor.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 100, '1s') at_modifier_gauge @ 0;

-- 2. `@ start()`: every step selects its samples around the start of the evaluation range
-- (`(-200s, 100s]`), so every step reports the same value per series (1.0 for 'a', 8.0 for 'b').
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 400, '100s') at_modifier_gauge @ start();

-- Control: without `@` every step looks back from its own timestamp, so the steps differ.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 400, '100s') at_modifier_gauge;

-- 3. `@ end()`: every step selects its samples around the end of the evaluation range
-- (`(100s, 400s]`), whose newest samples are 6.0 (a) and 13.0 (b) at 360s.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 400, '100s') at_modifier_gauge @ end();

-- 4. Range selector anchored by `@`: the call directly above the anchored range selector is
-- evaluated once, at the start of the evaluation, and its result is reported at every step — the
-- planner's counterpart of Prometheus' `StepInvariantExpr`. The window `(0s, 300s]` is left-open,
-- so it holds 60 -> 300 for host 'a' and 130 -> 250 for host 'b', i.e. 240 and 120 units. The 60s
-- gap to the window start is below the extrapolation threshold (1.1 x the 60s sampling interval),
-- so `rate` adds it and divides by the 300s window: 240 * 1.25 / 300 = 1.0 and
-- 120 * 1.25 / 300 = 0.5 per second. Every step must report those same per-series values, and both
-- series must be there at every step.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 240, '60s') rate(at_modifier_counter_total[5m] @ 300);

-- Consistency: every step of the query above must equal the anchored value of its own series, as a
-- separate instant query. The instant query below folds the same left-open `(0s, 300s]` window and
-- reports the anchored values (1.0 per second for 'a', 0.5 for 'b'), so the comparison after it
-- must stay empty for the matching series: a `rate` that followed the outer evaluation timestamp
-- instead of the anchor would report values other than 1.0 at some steps and show up there, and a
-- series that lost its anchor (or was read from the wrong one) shows up there as well.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (300, 300, '1s') rate(at_modifier_counter_total[5m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 240, '60s') rate(at_modifier_counter_total[5m] @ 300) != 1.0;

-- Parentheses around the range argument are transparent: the call must be promoted exactly like
-- the unparenthesized one above, so every step reports the same anchored values for both series
-- (1.0 for host 'a', 0.5 for host 'b') instead of folding a window per step.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 240, '60s') rate((at_modifier_counter_total[5m] @ 300));

-- The same holds for `@ start()` and `@ end()`, which are also fixed anchors: they resolve to the
-- statement's evaluation range, so a range call using them is evaluated once as well.
-- The statement starts at 60s: the window `(-240s, 60s]` holds only the samples at 0s and 60s, so
-- host 'a' goes 0 -> 60 and host 'b' 100 -> 130. The far 240s gap to the window start is beyond the
-- extrapolation threshold (1.1 x the 60s interval), so only half an interval (30s) is added there;
-- host 'a' starts its counter at 0, which clamps that back: 60 / 300 = 0.2 and
-- 30 * 1.5 / 300 = 0.15 per second.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (60, 300, '60s') rate(at_modifier_counter_total[5m] @ start());

-- The instant query at 60s folds the same window as every step of the query above.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (60, 60, '1s') rate(at_modifier_counter_total[5m]);

-- The statement ends at 240s: the window `(-60s, 240s]` holds the samples 0s to 240s, so host 'a'
-- goes 0 -> 240 and host 'b' 100 -> 220. The 60s gap to the window start is added for both, but
-- host 'a' starts at zero, which clamps it back (240 / 300 = 0.8 per second) while host 'b' keeps
-- it (120 * 1.25 / 300 = 0.5 per second).
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 240, '60s') rate(at_modifier_counter_total[5m] @ end());

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (240, 240, '1s') rate(at_modifier_counter_total[5m]);

-- Control: without `@` each step folds its own window, so the values keep changing.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 240, '60s') rate(at_modifier_counter_total[5m]);

-- `@ start()` on a range selector: the window `(-300s, 0s]` is left-open, so it holds a single
-- sample per series — the one exactly at the anchor — and `count_over_time` returns 1 at every step
-- for both of them.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 300, '100s') count_over_time(at_modifier_counter_total[5m] @ start());

-- Control: without `@` the window keeps moving, so the count grows with the step.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 300, '100s') count_over_time(at_modifier_counter_total[5m]);

-- 5. `@` combined with `offset`: the offset moves the anchor backwards before the window is
-- selected, so `@ 300 offset 2m` reports the samples at 180s.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 100, '1s') at_modifier_gauge @ 300 offset 2m;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 100, '1s') at_modifier_gauge @ 300 offset 1m;

-- Control: the offset applies to the evaluation timestamp when there is no `@`.
TQL EVAL (100, 100, '1s') at_modifier_gauge offset 2m;

-- 6. `@` inside a larger expression. The anchored operand contributes the same value at every
-- step, while the plain operand still follows the evaluation step. A binary expression is never
-- promoted: when one side is anchored and the other is not, the anchored side replays on its own,
-- and when both sides are anchored, each side reports its own anchored samples. The join then runs
-- at every step over the replayed per-series inputs.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 400, '100s') at_modifier_gauge @ 300 + at_modifier_gauge @ 0;

-- The aggregation above the anchored selector runs at every step over the replayed (still
-- per-series) samples, so it sums both series at every step.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 400, '100s') sum(at_modifier_gauge @ 0);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 400, '100s') abs(at_modifier_gauge @ 0);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 400, '100s') at_modifier_gauge @ 300 + at_modifier_gauge;

-- 7. `@ -1`, an anchor before the Unix epoch, is accepted. Its window `(-301s, -1s]` holds no
-- sample, so the result is empty.
TQL EVAL (0, 0, '1s') at_modifier_gauge @ -1;

-- 8. `@` on a metric that does not exist yields an empty result.
TQL EVAL (100, 100, '1s') at_modifier_missing @ 300;

-- The same for a range selector over a missing metric: the anchored window is folded once by the
-- promoted call, but the scan has no row to fold, so the whole grid stays empty.
TQL EVAL (0, 240, '60s') rate(at_modifier_missing[5m] @ 300);

-- 9. Multi-series coverage of the promoted call. The call above the anchored range selector is
-- evaluated once per series and its result is replayed per series, so grouping by `host` must
-- report both groups at every step: 1.0 for host 'a' and 0.5 for host 'b'. A promoted result that
-- treated the two series as one timeline would report the first step from the first row of the
-- batch and the later steps from its last row, dropping or mixing a host here.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 240, '60s') sum by (host) (rate(at_modifier_counter_total[5m] @ 300));

-- Consistency: comparing the per-host sequence above against 1.0 keeps exactly host 'b' at
-- every step, confirming it stays at 0.5 for the whole grid.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 240, '60s') sum by (host) (rate(at_modifier_counter_total[5m] @ 300)) != 1.0;

-- A value function directly over an anchored instant selector is not promoted: the selector
-- anchors and replays its sample per series, and `abs` is evaluated at every step over that replay.
-- It must keep one row per series per step.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (100, 400, '100s') abs(at_modifier_gauge @ 300);

-- 10. `predict_linear` over an anchored range selector: the regression is centered on the
-- evaluation instant of the step, not on the last sample of the window. The window `(0s, 300s]` of
-- host 'a' is left-open (the sample at 0s is outside it) and rises by 1.0 per minute, so each step
-- predicts 60s ahead of the trend read at its own instant: 6.0 at 300s and 7.0 at 360s.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (300, 360, '60s') predict_linear(at_modifier_gauge{host="a"}[5m] @ 300, 60);

-- The anchor and the start of the evaluation differ here, so the window `(-60s, 240s]` — samples 0s
-- to 240s — is shifted by `at_offset` = 300s - 240s = 60s. Every step must follow the evaluation
-- grid (6.0 to 11.0); a regression read at the wrong instant predicts 60s ahead of the window's
-- last sample instead, i.e. 5.0 at every step — the sample at 240s is 4.0 and the trend rises by
-- 1.0 per minute — and shows up as a constant here.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (300, 600, '60s') predict_linear(at_modifier_gauge{host="a"}[5m] @ 240, 60);

-- Control: without `@` every step folds its own window, whose last sample is up to a step older
-- than the evaluation instant. Reading the regression off that sample reports 6.0 / 7.0 instead
-- of 6.5 / 7.5 here.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (330, 390, '60s') predict_linear(at_modifier_gauge{host="a"}[5m], 60);

-- The same holds with an `offset`, which shifts the whole window backwards without moving the
-- evaluation instants: the window of step `T` is the left-open `(T - 1m - 5m, T - 1m]`, and the
-- trend is read at `T`, i.e. one minute past its newest sample.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (330, 390, '60s') predict_linear(at_modifier_gauge{host="a"}[5m] offset 1m, 60);

-- `timestamp()` of an anchored instant selector is not promoted either: its argument is an
-- instant selector, not a range selector, so the selector anchors and replays its sample per series
-- on its own and `timestamp()` reports the timestamp of the sample the anchor selected at every
-- step (300s here). Its plan keeps the anchored selection below the replay of the grid and projects
-- the timestamp above it.
--
-- Only an `@` without an `offset` is asserted here: the value of `timestamp()` for an anchored
-- selector that also carries an `offset` is a pre-existing question of its own, out of scope here.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 240, '60s') timestamp(at_modifier_gauge @ 300);

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
TQL EXPLAIN (0, 240, '60s') timestamp(at_modifier_gauge @ 300);

-- 11. `label_join` above an anchored selector. The call rewrites the label the input series are
-- told apart by (`host` becomes the empty string), so it is never promoted: it must stay above the
-- per-series replay of the selector and be evaluated at every step. Both hosts are still reported,
-- with their own value, at every step. A promoted `label_join` would instead be replayed through
-- the labels it just rewrote and report the two series as a single timeline, i.e. one mixed row
-- per step.
--
-- The invariant asserted here is scoped to the replay: it must not drop or mix the input rows.
-- It is not a claim about the final PromQL semantics of this query: joining `host` to one value
-- leaves two samples with the same label set at the same timestamp, which Prometheus rejects,
-- while `label_join` does not validate that yet (`label_replace` errors on such a rewrite
-- instead). That duplicate-labelset validation gap is pre-existing and out of scope here.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (300, 480, '60s') label_join(at_modifier_gauge @ 300, "host", "", "");

-- The same join below another call: neither is promoted, and both are evaluated at every step over
-- the per-series replay of the anchored selector.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (300, 480, '60s') abs(label_join(at_modifier_gauge @ 300, "host", "", ""));

-- A range call below the join is still promoted on its own (it is the direct call over the
-- anchored range selector): the anchored window is folded once per series, and the join above that
-- replay reports the rate of both hosts at every step.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (300, 480, '60s') label_join(rate(at_modifier_counter_total[5m] @ 300), "host", "", "");

DROP TABLE at_modifier_gauge;

DROP TABLE at_modifier_counter_total;
