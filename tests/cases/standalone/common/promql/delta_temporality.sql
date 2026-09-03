CREATE TABLE delta_temporality (
    ts TIMESTAMP TIME INDEX,
    greptime_value DOUBLE,
    series STRING,
    otlp_aggregation_temporality STRING,
    PRIMARY KEY (series, otlp_aggregation_temporality)
);

INSERT INTO delta_temporality VALUES
    (60000, 10, 'delta', 'delta'),
    (120000, 20, 'delta', 'delta'),
    (180000, 15, 'delta', 'delta'),
    (180000, 7, 'single', 'delta'),
    (60000, 10, 'cumulative', NULL),
    (120000, 20, 'cumulative', NULL),
    (180000, 30, 'cumulative', NULL);

-- Raw deltas are summed; untagged rows retain cumulative reset-aware math.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (180, 180, '1m') increase(delta_temporality[3m]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (180, 180, '1m') rate(delta_temporality[3m]);

-- The physical plan selects raw-delta math per series while retaining cumulative rate.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (?m)^\|\s1_\|\s0_\|_(?:Projection|Filter)Exec:.*\n
-- SQLNESS REPLACE (?m)^\|_\|_\|_FilterExec:.*\n
-- SQLNESS REPLACE END\sas\s.*,\sseries@2\sas\sseries END as RATE_RESULT, series@2 as series
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
TQL ANALYZE (180, 180, '1m') rate(delta_temporality[3m]);

-- The reserved temporality marker treats NULL as the absent cumulative state.
TQL EVAL (180, 180, '1m') delta_temporality{otlp_aggregation_temporality=""};
TQL EVAL (180, 180, '1m') delta_temporality{otlp_aggregation_temporality!="delta"};

-- Generated vector plans retain timestamp broadcast across the temporality marker.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (180, 180, '1m') vector(2) * delta_temporality;
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (180, 180, '1m') vector(2) * ignoring(otlp_aggregation_temporality) delta_temporality;
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (180, 180, '1m') delta_temporality * vector(2);
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (180, 180, '1m') delta_temporality * ignoring(otlp_aggregation_temporality) vector(2);

-- Aggregation preserves or deliberately removes the visible stored marker.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (180, 180, '1m') sum by (series, otlp_aggregation_temporality) (rate(delta_temporality[3m]));
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (180, 180, '1m') sum by (series) (rate(delta_temporality[3m]));
TQL EVAL (180, 180, '1m') round(sum(rate(delta_temporality[3m])), 0.000001);

CREATE TABLE delta_marker_only (
    ts TIMESTAMP TIME INDEX,
    greptime_value DOUBLE,
    otlp_aggregation_temporality STRING PRIMARY KEY
);

INSERT INTO delta_marker_only VALUES
    (180000, 1, 'delta'),
    (180000, 2, NULL);

CREATE TABLE delta_tagless (
    ts TIMESTAMP TIME INDEX,
    greptime_value DOUBLE
);

INSERT INTO delta_tagless VALUES (180000, 10);

-- A missing temporality marker matches the NULL cumulative state; "delta" does not.
TQL EVAL (180, 180, '1m') delta_marker_only + delta_tagless;
TQL EVAL (180, 180, '1m') delta_marker_only AND delta_tagless;

DROP TABLE delta_tagless;
DROP TABLE delta_marker_only;
DROP TABLE delta_temporality;
