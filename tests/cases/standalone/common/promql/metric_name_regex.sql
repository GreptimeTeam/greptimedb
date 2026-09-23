-- A non-equality `__name__` matcher selects every metric table whose name it matches, and the
-- whole expression is evaluated once over all of them: `sum`/`topk`/`count` are global, not
-- per metric table.
--
-- Use the metric engine to match real PromQL usage. The two metric tables have different labels
-- and different values, so a per-table (instead of global) evaluation is visible in the result.
--
-- The tables live in a dedicated schema: a `!=` matcher names no prefix, so the candidates are
-- pinned with `__schema__` to keep the expected result independent of other cases.
CREATE SCHEMA metric_name_regex;

CREATE TABLE metric_name_regex.metric_name_regex_phy (
    ts TIMESTAMP TIME INDEX,
    greptime_value DOUBLE
) ENGINE=metric WITH ("physical_metric_table" = "");

CREATE TABLE metric_name_regex.metric_name_regex_a (
    host STRING NULL,
    ts TIMESTAMP NOT NULL,
    greptime_value DOUBLE NULL,
    TIME INDEX (ts),
    PRIMARY KEY (host)
) ENGINE=metric WITH (on_physical_table = 'metric_name_regex_phy');

CREATE TABLE metric_name_regex.metric_name_regex_b (
    idc STRING NULL,
    ts TIMESTAMP NOT NULL,
    greptime_value DOUBLE NULL,
    TIME INDEX (ts),
    PRIMARY KEY (idc)
) ENGINE=metric WITH (on_physical_table = 'metric_name_regex_phy');

INSERT INTO metric_name_regex.metric_name_regex_a (ts, host, greptime_value) VALUES (0, 'host1', 1);

INSERT INTO metric_name_regex.metric_name_regex_b (ts, idc, greptime_value) VALUES (0, 'idc1', 5);

-- A bare selector returns one series per metric table, each labelled with its own metric name.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') {__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"};

-- An equality matcher over the same tables keeps resolving to a single table.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') {__name__="metric_name_regex_a", __schema__="metric_name_regex"};

-- `sum` aggregates across every matched metric table: per-table evaluation could only ever
-- report one of the two values.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') sum({__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"});

-- `topk` ranks series across every matched metric table, so the globally largest series wins.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') topk(1, {__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"});

-- The metric name is an addressable label of the union result.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') count by(__name__) ({__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"});

-- Arithmetic computes new sample values, so it drops the metric name: grouping by `__name__`
-- then reports one group instead of one per metric table.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') count by(__name__) ({__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"} * 2);

-- A matcher that resolves to no metric table returns an empty result instead of an error.
TQL EVAL (0, 0, '1s') {__name__=~"metric_name_regex_nonexistent.*", __schema__="metric_name_regex"};

-- `!=` is a union as well. A selector carries at most one `__name__` matcher, so the region of
-- the metric space is pinned with `__schema__` instead of a second `__name__` matcher.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') {__name__!="metric_name_regex_b", __schema__="metric_name_regex"};

-- Vector matching compares the label set of each series: the union pads `_a` with an empty `idc`
-- and `_b` with an empty `host`, so `_a` matches the exact-name operand and `_b` does not.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') {__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"} + {__name__="metric_name_regex_a", __schema__="metric_name_regex"};

-- A set operator matches on the labels without the metric name, so the exact-name operand matches
-- the union's own `_a` series: `or` reports two rows, each labelled with the name it carries.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') {__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"} or metric_name_regex_a{__schema__="metric_name_regex"};

-- The operand order changes nothing: the exact-name operand reports its own name as well, and the
-- union's `_b` row is appended.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') metric_name_regex_a{__schema__="metric_name_regex"} or {__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"};

-- `and` keeps the left-hand series that the other operand matches on every label, where a label a
-- series does not carry is the empty value: only the union's `_a` row survives, name included.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') {__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"} and metric_name_regex_a{__schema__="metric_name_regex"};

-- `unless` removes that series instead and leaves the union's `_b` row.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') {__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"} unless metric_name_regex_a{__schema__="metric_name_regex"};

-- `on(__name__)` makes the name a matching label, so the exact-name operand de-duplicates against
-- the union's row of the same name instead of being appended as a second `_b` series.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') {__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"} or on(__name__) metric_name_regex_b{__schema__="metric_name_regex"};

-- A filtering comparison keeps the left-hand samples whose labels match: `on(host)` compares `host`
-- alone and the union's `_b` row carries the empty `host`, so only `_a` survives. Prometheus drops
-- the metric name of a comparison that matches `on(...)`, so `_a` is reported without a name.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') {__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"} >= on(host) metric_name_regex_a{__schema__="metric_name_regex"};

-- A scalar comparison filters the union without matching against another series, so every sample
-- that satisfies it is kept, name included.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') {__name__=~"metric_name_regex_[ab]", __schema__="metric_name_regex"} > 0;

-- Candidate metric tables may store their time index at different precisions: `_c` is a
-- `timestamp(9)` table next to the millisecond `_a`/`_b`. The union aligns every branch to the
-- finest candidate unit, so a millisecond sample keeps its instant through the cast.
CREATE TABLE metric_name_regex.metric_name_regex_phy_ns (
    ts TIMESTAMP(9) TIME INDEX,
    greptime_value DOUBLE
) ENGINE=metric WITH ("physical_metric_table" = "");

CREATE TABLE metric_name_regex.metric_name_regex_c (
    host STRING NULL,
    ts TIMESTAMP(9) NOT NULL,
    greptime_value DOUBLE NULL,
    TIME INDEX (ts),
    PRIMARY KEY (host)
) ENGINE=metric WITH (on_physical_table = 'metric_name_regex_phy_ns');

-- One sample at the epoch and one sub-millisecond past 1s (1s + 1ns).
INSERT INTO metric_name_regex.metric_name_regex_c (ts, host, greptime_value) VALUES (0, 'host1', 3), (1000000001, 'host1', 4);

-- Every candidate is scanned, including the nanosecond one, and no branch is rejected.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') {__name__=~"metric_name_regex_[abc]", __schema__="metric_name_regex"};

-- `sum` aggregates across the mixed precision tables as well.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') sum({__name__=~"metric_name_regex_[abc]", __schema__="metric_name_regex"});

-- The aligned time index keeps native nanosecond resolution: the 1s + 1ns sample is past the
-- `1s` lookback boundary, so it belongs to the 2s step only. Aligning the branch to milliseconds
-- would truncate it onto the boundary and drop it.
TQL EVAL (2, 2, '1s', '1s') {__name__=~"metric_name_regex_[abc]", __schema__="metric_name_regex"};

DROP TABLE metric_name_regex.metric_name_regex_a;

DROP TABLE metric_name_regex.metric_name_regex_b;

DROP TABLE metric_name_regex.metric_name_regex_c;

DROP TABLE metric_name_regex.metric_name_regex_phy;

DROP TABLE metric_name_regex.metric_name_regex_phy_ns;

DROP SCHEMA metric_name_regex;
