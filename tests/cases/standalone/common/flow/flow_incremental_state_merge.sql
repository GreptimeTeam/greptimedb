-- Incremental aggregate state merge over a partitioned source (two regions).
-- A WHERE-filtered flow mixes scalar aggregates (sum, count) with aggregate
-- state columns (hll, stddev_pop_state, uddsketch_state, avg_state); each later
-- delta in the same time window must merge its state with the state already persisted
-- in the sink. Covers two nonempty deltas across both regions, one round whose
-- delta is empty because the flow's predicate drops the newly inserted row,
-- and a final nonempty delta checked against a direct source aggregation.
CREATE TABLE flow_incr_state_merge_input (
    host_id INT,
    user_id BIGINT,
    n INT,
    v DOUBLE,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(host_id, user_id)
)
PARTITION ON COLUMNS (host_id) (
    host_id < 3,
    host_id >= 3
)
WITH (
    append_mode = 'true'
);

CREATE FLOW flow_incr_state_merge SINK TO flow_incr_state_merge_sink
WITH (experimental_enable_incremental_read = 'true')
AS
SELECT
    sum(n) AS total,
    count(n) AS row_count,
    hll(user_id) AS user_hll,
    stddev_pop_state(v) AS v_stddev_state,
    uddsketch_state(128, 0.01, v) AS v_sketch_state,
    avg_state(v) AS v_avg_state,
    date_bin(INTERVAL '1 minute', ts, '2024-01-01 00:00:00') AS time_window
FROM
    flow_incr_state_merge_input
WHERE
    v > 0
GROUP BY
    time_window;

-- ==== Phase 1: initial rows in the same time window, one per region ====
INSERT INTO flow_incr_state_merge_input VALUES
    (1, 1, 1, 1.0, '2024-01-01 00:00:00'),
    (3, 2, 2, 2.0, '2024-01-01 00:00:10');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_incr_state_merge');

-- The first round seeds the sink with a full-snapshot aggregate state.
SELECT
    total,
    row_count,
    time_window
FROM
    flow_incr_state_merge_sink
ORDER BY
    time_window;

-- Move the checkpointed source rows and the seeded sink state into SST, so
-- the next round has to merge a new delta with persisted state.
ADMIN FLUSH_TABLE('flow_incr_state_merge_input');

ADMIN FLUSH_TABLE('flow_incr_state_merge_sink');

-- ==== Phase 2: first nonempty delta, same window, both regions ====
INSERT INTO flow_incr_state_merge_input VALUES
    (2, 3, 3, 3.0, '2024-01-01 00:00:20'),
    (4, 4, 4, 4.0, '2024-01-01 00:00:40');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_incr_state_merge');

-- Scalars accumulate; each state column must merge with the persisted state.
SELECT
    total,
    row_count,
    hll_count(hll_merge(user_hll)) AS users,
    ROUND(stddev_pop_calc(stddev_pop_merge(v_stddev_state)), 6) AS v_stddev,
    ROUND(uddsketch_calc(0.5, uddsketch_merge(128, 0.01, v_sketch_state)), 4) AS v_p50,
    ROUND(avg_calc(avg_merge(v_avg_state)), 6) AS v_avg,
    time_window
FROM
    flow_incr_state_merge_sink
GROUP BY
    total,
    row_count,
    time_window
ORDER BY
    time_window;

ADMIN FLUSH_TABLE('flow_incr_state_merge_input');

ADMIN FLUSH_TABLE('flow_incr_state_merge_sink');

-- ==== Phase 3: filtered-out row must run one round with an empty delta ====
-- Record the flow's last execution time before the excluded insert: the
-- comparison below proves the empty delta was actually evaluated.
CREATE TABLE flow_incr_state_merge_last_exec (
    marker STRING,
    last_exec TIMESTAMP TIME INDEX
) WITH (
    append_mode = 'true'
);

-- The background loop runs on its own interval; keep waiting until it has
-- executed at least once so information_schema.flows.last_execution_time is
-- non-null before snapshotting it.
-- SQLNESS SLEEP 8s
INSERT INTO flow_incr_state_merge_last_exec (marker, last_exec)
SELECT
    'before_empty_delta',
    last_execution_time
FROM
    information_schema.flows
WHERE
    flow_name = 'flow_incr_state_merge';

-- The row's timestamp is inside the existing window, so the source write still
-- dirties that window; the flow's WHERE predicate then drops the row from the
-- delta, leaving the delta empty for the window.
INSERT INTO flow_incr_state_merge_input VALUES
    (1, 9, 9, -1.0, '2024-01-01 00:00:30');

-- SQLNESS SLEEP 6s
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_incr_state_merge');

-- An empty delta must leave the persisted sink states untouched.
SELECT
    total,
    row_count,
    hll_count(hll_merge(user_hll)) AS users,
    ROUND(stddev_pop_calc(stddev_pop_merge(v_stddev_state)), 6) AS v_stddev,
    ROUND(uddsketch_calc(0.5, uddsketch_merge(128, 0.01, v_sketch_state)), 4) AS v_p50,
    ROUND(avg_calc(avg_merge(v_avg_state)), 6) AS v_avg,
    time_window
FROM
    flow_incr_state_merge_sink
GROUP BY
    total,
    row_count,
    time_window
ORDER BY
    time_window;

-- last_execution_time must have advanced past the mark taken above, proving
-- the dirty window triggered a new round even though its delta was empty. A
-- NULL comparison means the mark never advanced: fail explicitly instead of
-- silently treating it as "unchanged".
INSERT INTO flow_incr_state_merge_last_exec (marker, last_exec)
SELECT
    'after_empty_delta',
    last_execution_time
FROM
    information_schema.flows
WHERE
    flow_name = 'flow_incr_state_merge';

SELECT
    (
        SELECT
            last_exec
        FROM
            flow_incr_state_merge_last_exec
        WHERE
            marker = 'after_empty_delta'
    ) > (
        SELECT
            last_exec
        FROM
            flow_incr_state_merge_last_exec
        WHERE
            marker = 'before_empty_delta'
    ) AS empty_delta_executed;

-- Flush the input before the final delta so each phase's delta is isolated in
-- its own round; the background loop may have already consumed the last
-- insert, which is fine — the values below only depend on the merge outcome.

ADMIN FLUSH_TABLE('flow_incr_state_merge_input');

ADMIN FLUSH_TABLE('flow_incr_state_merge_sink');

-- ==== Phase 4: final nonempty delta, same window, both regions ====
INSERT INTO flow_incr_state_merge_input VALUES
    (1, 5, 5, 5.0, '2024-01-01 00:00:05'),
    (3, 6, 6, 6.0, '2024-01-01 00:00:55');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_incr_state_merge');

-- Merge the final delta with the persisted states and compare the merged
-- result against a direct aggregation over the accepted source rows.
SELECT
    merged.total = direct.total AS total_matches,
    merged.row_count = direct.row_count AS count_matches,
    merged.users = direct.users AS hll_matches,
    abs(merged.v_stddev - direct.v_stddev) < 1e-9 AS stddev_matches,
    ROUND(merged.v_p50, 4) = ROUND(direct.v_p50, 4) AS p50_matches,
    abs(merged.v_avg - direct.v_avg) < 1e-9 AS avg_matches
FROM
    (
        SELECT
            total,
            row_count,
            hll_count(hll_merge(user_hll)) AS users,
            stddev_pop_calc(stddev_pop_merge(v_stddev_state)) AS v_stddev,
            uddsketch_calc(0.5, uddsketch_merge(128, 0.01, v_sketch_state)) AS v_p50,
            avg_calc(avg_merge(v_avg_state)) AS v_avg
        FROM
            flow_incr_state_merge_sink
        GROUP BY
            total,
            row_count
    ) AS merged,
    (
        SELECT
            sum(n) AS total,
            count(n) AS row_count,
            hll_count(hll(user_id)) AS users,
            stddev_pop(v) AS v_stddev,
            uddsketch_calc(0.5, uddsketch_state(128, 0.01, v)) AS v_p50,
            avg(v) AS v_avg
        FROM
            flow_incr_state_merge_input
        WHERE
            v > 0
    ) AS direct;

-- Clean up
DROP FLOW flow_incr_state_merge;

DROP TABLE flow_incr_state_merge_input;

DROP TABLE flow_incr_state_merge_sink;

DROP TABLE flow_incr_state_merge_last_exec;
