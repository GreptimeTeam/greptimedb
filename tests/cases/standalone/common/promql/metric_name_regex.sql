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

-- A matcher that resolves to no metric table returns an empty result instead of an error.
TQL EVAL (0, 0, '1s') {__name__=~"metric_name_regex_nonexistent.*", __schema__="metric_name_regex"};

-- `!=` is a union as well. A selector carries at most one `__name__` matcher, so the region of
-- the metric space is pinned with `__schema__` instead of a second `__name__` matcher.
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 0, '1s') {__name__!="metric_name_regex_b", __schema__="metric_name_regex"};

DROP TABLE metric_name_regex.metric_name_regex_a;

DROP TABLE metric_name_regex.metric_name_regex_b;

DROP TABLE metric_name_regex.metric_name_regex_phy;

DROP SCHEMA metric_name_regex;
