-- A plain projection/filter flow without a time window reads only source rows
-- written since the last checkpoint when incremental reads are opted in.
--
-- `ADMIN FLUSH_FLOW` returns the number of rows the round wrote to the sink, so
-- the numbers below are the assertion that a round read only the delta: a round
-- that rescanned history would rewrite every already-materialized row
-- (2 + 2 = 4 after the second round instead of 2).
--
-- The source must be append-only and preserve row sequence. The sink is a keyed
-- materialization: two rows sharing the sink key collapse.
CREATE TABLE flow_incremental_simple_input (
    host STRING,
    val DOUBLE,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(host)
) WITH (
    append_mode = 'true',
    preserve_row_sequence = 'true'
);

-- A long interval keeps every round explicit, so the row counts are stable.
CREATE FLOW flow_incremental_simple
SINK TO flow_incremental_simple_sink
EVAL INTERVAL '1h'
WITH (experimental_enable_incremental_read = 'true')
AS
SELECT host, val, ts FROM flow_incremental_simple_input WHERE val >= 0;

-- Both hosts share one timestamp, so the sink keeps two rows and its time index
-- is the source time index rather than the first timestamp column it finds.
INSERT INTO flow_incremental_simple_input VALUES
    ('host_a', 1.0, '2024-01-01 00:00:00'),
    ('host_b', 2.0, '2024-01-01 00:00:00');

ADMIN FLUSH_FLOW('flow_incremental_simple');

SELECT host, val, ts FROM flow_incremental_simple_sink ORDER BY host, ts;

SHOW CREATE TABLE flow_incremental_simple_sink;

-- Move the checkpointed source rows into SST files so the next round must use
-- the exact sequence range instead of rescanning them.
ADMIN FLUSH_TABLE('flow_incremental_simple_sink');
ADMIN FLUSH_TABLE('flow_incremental_simple_input');

INSERT INTO flow_incremental_simple_input VALUES
    ('host_a', 3.0, '2024-01-01 00:01:00'),
    ('host_c', 4.0, '2024-01-01 00:01:00');

ADMIN FLUSH_TABLE('flow_incremental_simple_input');

ADMIN FLUSH_FLOW('flow_incremental_simple');

SELECT host, val, ts FROM flow_incremental_simple_sink ORDER BY host, ts;

-- A fully filtered round writes nothing; its scan still proves a watermark, so
-- the checkpoint advances and the following insert must be processed.
INSERT INTO flow_incremental_simple_input VALUES
    ('host_a', -1.0, '2024-01-01 00:02:00');

ADMIN FLUSH_FLOW('flow_incremental_simple');

SELECT host, val, ts FROM flow_incremental_simple_sink ORDER BY host, ts;

INSERT INTO flow_incremental_simple_input VALUES
    ('host_d', 5.0, '2024-01-01 00:03:00');

ADMIN FLUSH_FLOW('flow_incremental_simple');

SELECT host, val, ts FROM flow_incremental_simple_sink ORDER BY host, ts;

-- Without the opt-in option a window-less flow keeps rescanning history: the same
-- second round rewrites all four rows instead of only the two new ones.
CREATE FLOW flow_incremental_simple_full
SINK TO flow_incremental_simple_full_sink
EVAL INTERVAL '1h'
AS
SELECT host, val, ts FROM flow_incremental_simple_input WHERE val >= 0;

INSERT INTO flow_incremental_simple_input VALUES
    ('host_e', 6.0, '2024-01-01 00:04:00');

ADMIN FLUSH_FLOW('flow_incremental_simple_full');

SELECT host, val, ts FROM flow_incremental_simple_full_sink ORDER BY host, ts;

INSERT INTO flow_incremental_simple_input VALUES
    ('host_f', 7.0, '2024-01-01 00:05:00');

ADMIN FLUSH_FLOW('flow_incremental_simple_full');

SELECT host, val, ts FROM flow_incremental_simple_full_sink ORDER BY host, ts;

DROP FLOW flow_incremental_simple;
DROP FLOW flow_incremental_simple_full;
DROP TABLE flow_incremental_simple_input;
DROP TABLE flow_incremental_simple_sink;
DROP TABLE flow_incremental_simple_full_sink;
