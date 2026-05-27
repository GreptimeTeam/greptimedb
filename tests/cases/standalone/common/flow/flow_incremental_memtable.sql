-- Validate that a flow performing an incremental aggregate read only reads memtable
-- data and does NOT re-read source rows that have already been flushed to SST after
-- a previous checkpoint.
CREATE TABLE flow_incr_memtable_input (
    host_id INT,
    n INT,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(host_id)
) WITH (
    append_mode = 'true'
);

CREATE FLOW flow_incr_memtable SINK TO flow_incr_memtable_sink AS
SELECT
    sum(n) AS total,
    min(n) AS min_n,
    max(n) AS max_n,
    date_bin(INTERVAL '1 minute', ts, '2024-01-01 00:00:00') AS time_window
FROM
    flow_incr_memtable_input
GROUP BY
    time_window;

-- ==== Phase 1: initial insert + checkpoint ====
INSERT INTO flow_incr_memtable_input VALUES
    (1, 10, '2024-01-01 00:00:00'),
    (2, 20, '2024-01-01 00:00:30');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_incr_memtable');

SELECT total, min_n, max_n, time_window FROM flow_incr_memtable_sink ORDER BY time_window;

-- ==== Phase 2: flush source table to SST ====
-- This moves the already-checkpointed data into SST files.
ADMIN FLUSH_TABLE('flow_incr_memtable_input');

-- ==== Phase 3: insert new delta within the same time window ====
INSERT INTO flow_incr_memtable_input VALUES
    (3, 30, '2024-01-01 00:00:15'),
    (4, 40, '2024-01-01 00:00:45');

-- ==== Phase 4: flush flow again (incremental read) ====
-- The flow must only read the new memtable delta and merge with the existing
-- sink aggregate. If it mistakenly re-reads the SST, the result will be
-- inflated (initial data counted twice).
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_incr_memtable');

SELECT total, min_n, max_n, time_window FROM flow_incr_memtable_sink ORDER BY time_window;

-- Clean up
DROP FLOW flow_incr_memtable;
DROP TABLE flow_incr_memtable_input;
DROP TABLE flow_incr_memtable_sink;
