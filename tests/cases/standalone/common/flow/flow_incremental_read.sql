-- Exercises the complete batching-flow path for PR3 incremental reads:
-- CREATE FLOW -> manual flush -> checkpointed incremental flush -> sink reads.
CREATE TABLE flow_inc_read_input (
    host STRING,
    val DOUBLE,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(host)
);

CREATE FLOW flow_inc_read_sum SINK TO flow_inc_read_sink AS
SELECT
    host,
    sum(val) AS val_sum,
    date_bin(INTERVAL '1 second', ts, '2024-01-01 00:00:00') AS time_window
FROM
    flow_inc_read_input
GROUP BY
    host,
    time_window;

INSERT INTO flow_inc_read_input VALUES
    ('a', 1.0, '2024-01-01 00:00:00.100'),
    ('b', 2.0, '2024-01-01 00:00:00.200');

-- First flush is a full snapshot. It should collect region watermarks and arm
-- the flow for sequence-based incremental reads in the next flush.
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_inc_read_sum');

SELECT
    host,
    val_sum,
    time_window
FROM
    flow_inc_read_sink
ORDER BY
    host,
    time_window;

INSERT INTO flow_inc_read_input VALUES
    ('a', 3.0, '2024-01-01 00:00:01.100'),
    ('b', 4.0, '2024-01-01 00:00:01.200');

-- Second flush runs through the same public Flow process but should be issued
-- with PR3 incremental extensions internally because the previous flush
-- advanced checkpoints.
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_inc_read_sum');

SELECT
    host,
    val_sum,
    time_window
FROM
    flow_inc_read_sink
ORDER BY
    host,
    time_window;

INSERT INTO flow_inc_read_input VALUES
    ('a', 3.0, '2024-01-01 00:00:00.300');

-- Third flush targets an existing (host, time_window) group. Incremental
-- aggregate merge should combine the new delta with the existing sink row.
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_inc_read_sum');

SELECT
    host,
    val_sum,
    time_window
FROM
    flow_inc_read_sink
ORDER BY
    host,
    time_window;

DROP FLOW flow_inc_read_sum;
DROP TABLE flow_inc_read_sink;
DROP TABLE flow_inc_read_input;
