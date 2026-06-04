-- Validate that a flow performing an incremental aggregate read on a
-- partitioned source table (multiple regions) only reads memtable data
-- and does NOT re-read source rows that have already been flushed to SST.
CREATE TABLE flow_incr_part_input (
    host_id INT,
    n INT,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(host_id)
)
PARTITION ON COLUMNS (host_id) (
    host_id < 3,
    host_id >= 3
)
WITH (
    append_mode = 'true'
);

CREATE FLOW flow_incr_part SINK TO flow_incr_part_sink
WITH (experimental_enable_incremental_read = 'true')
AS
SELECT
    sum(n) AS total,
    min(n) AS min_n,
    max(n) AS max_n,
    date_bin(INTERVAL '1 minute', ts, '2024-01-01 00:00:00') AS time_window
FROM
    flow_incr_part_input
GROUP BY
    time_window;

-- ==== Phase 1: initial insert across both partitions ====
INSERT INTO flow_incr_part_input VALUES
    (1, 10, '2024-01-01 00:00:00'),
    (4, 20, '2024-01-01 00:00:30');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_incr_part');

SELECT total, min_n, max_n, time_window FROM flow_incr_part_sink ORDER BY time_window;

-- ==== Phase 2: flush source table to SST ====
-- Move already checkpointed source rows into SST so the next incremental run
-- must skip them.
ADMIN FLUSH_TABLE('flow_incr_part_input');

-- ==== Phase 3: insert new delta across both partitions, same time window ====
INSERT INTO flow_incr_part_input VALUES
    (2, 30, '2024-01-01 00:00:15'),
    (3, 40, '2024-01-01 00:00:45');

-- ==== Phase 4: flush flow again (incremental read) ====
-- The flow must only read the new memtable delta from both regions and merge
-- with the existing sink aggregate. If it mistakenly re-reads the SST, the
-- result will be inflated (initial data counted twice).
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_incr_part');

SELECT total, min_n, max_n, time_window FROM flow_incr_part_sink ORDER BY time_window;

-- Clean up
DROP FLOW flow_incr_part;
DROP TABLE flow_incr_part_input;
DROP TABLE flow_incr_part_sink;
