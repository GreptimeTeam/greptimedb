CREATE TABLE incremental_aggr_input (
    host_id INT,
    n INT,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(host_id)
) WITH (
    append_mode = 'true'
);

CREATE FLOW incremental_aggr_flow SINK TO incremental_aggr_sink AS
SELECT
    sum(n) AS total,
    min(n) AS min_n,
    max(n) AS max_n,
    date_bin(INTERVAL '1 minute', ts, '2024-01-01 00:00:00') AS time_window
FROM
    incremental_aggr_input
GROUP BY
    time_window;

INSERT INTO incremental_aggr_input VALUES
    (1, 10, '2024-01-01 00:00:00'),
    (2, 20, '2024-01-01 00:00:30');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('incremental_aggr_flow');

SELECT total, min_n, max_n, time_window FROM incremental_aggr_sink ORDER BY time_window;

-- Move already checkpointed source rows into SST. The next incremental run
-- must still read only the memtable delta and must not merge these old SST
-- rows again.
ADMIN FLUSH_TABLE('incremental_aggr_input');

-- Insert more rows into the same time window. An incremental-safe flow should
-- merge the delta aggregate with the existing sink aggregate state.
INSERT INTO incremental_aggr_input VALUES
    (3, 30, '2024-01-01 00:00:15'),
    (4, 40, '2024-01-01 00:00:45');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('incremental_aggr_flow');

SELECT total, min_n, max_n, time_window FROM incremental_aggr_sink ORDER BY time_window;

-- Insert a row into a new time window to cover append of a new aggregate key.
INSERT INTO incremental_aggr_input VALUES
    (5, 50, '2024-01-01 00:01:00');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('incremental_aggr_flow');

SELECT total, min_n, max_n, time_window FROM incremental_aggr_sink ORDER BY time_window;

DROP FLOW incremental_aggr_flow;
DROP TABLE incremental_aggr_input;
DROP TABLE incremental_aggr_sink;
