-- Validate incremental sequence_range reads for an append-only source whose rows
-- preserve insertion sequence across SST flushes.
CREATE TABLE flow_incr_seq_range_input (
    host_id INT,
    n INT,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(host_id)
) WITH (
    append_mode = 'true',
    preserve_row_sequence = 'true'
);

CREATE FLOW flow_incr_seq_range SINK TO flow_incr_seq_range_sink
WITH (experimental_enable_incremental_read = 'true')
AS
SELECT
    sum(n) AS total,
    count(n) AS row_count,
    min(n) AS min_n,
    max(n) AS max_n,
    date_bin(INTERVAL '1 minute', ts, '2024-01-01 00:00:00') AS time_window
FROM
    flow_incr_seq_range_input
GROUP BY
    time_window;

-- ==== Phase 1: initial insert + checkpoint ====
INSERT INTO flow_incr_seq_range_input VALUES
    (1, 10, '2024-01-01 00:00:00'),
    (2, 20, '2024-01-01 00:00:15'),
    (3, 30, '2024-01-01 00:00:30');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_incr_seq_range');

SELECT total, row_count, min_n, max_n, time_window
FROM flow_incr_seq_range_sink
ORDER BY time_window;

-- Move the checkpointed source and sink state into SST files.
ADMIN FLUSH_TABLE('flow_incr_seq_range_sink');
ADMIN FLUSH_TABLE('flow_incr_seq_range_input');

-- ==== Phase 2: flushed delta in the same window ====
INSERT INTO flow_incr_seq_range_input VALUES
    (4, 40, '2024-01-01 00:00:45'),
    (5, 50, '2024-01-01 00:00:55');

-- Flush the delta to SST before the incremental flow run. The sequence range
-- scan must read only these new rows, not the already-checkpointed SST.
ADMIN FLUSH_TABLE('flow_incr_seq_range_input');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_incr_seq_range');

SELECT total, row_count, min_n, max_n, time_window
FROM flow_incr_seq_range_sink
ORDER BY time_window;

-- ==== Empty incremental run ====
-- No new source rows must leave the aggregate unchanged.
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_incr_seq_range');

SELECT total, row_count, min_n, max_n, time_window
FROM flow_incr_seq_range_sink
ORDER BY time_window;

DROP FLOW flow_incr_seq_range;
DROP TABLE flow_incr_seq_range_input;
DROP TABLE flow_incr_seq_range_sink;
