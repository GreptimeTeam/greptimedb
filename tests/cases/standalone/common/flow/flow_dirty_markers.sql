CREATE TABLE dirty_marker_source (
    series STRING,
    event_ts TIMESTAMP DEFAULT '1970-01-01 00:03:17',
    unrelated_ts TIMESTAMP,
    amount BIGINT,
    payload STRING,
    PRIMARY KEY (series),
    TIME INDEX (event_ts)
);

CREATE FLOW dirty_marker_flow SINK TO dirty_marker_sink AS
SELECT
    series,
    date_bin(INTERVAL '1 minute', event_ts) AS window_start,
    COUNT(*) AS row_count,
    SUM(amount) AS amount_sum
FROM dirty_marker_source
GROUP BY series, window_start;

INSERT INTO dirty_marker_source
    (series, event_ts, unrelated_ts, amount, payload)
VALUES
    ('alpha', '1969-12-31 23:59:59', '2024-01-01 00:00:00', 2,
     'unused payload large enough to remain outside the projection'),
    ('alpha', '1970-01-01 00:00:01', '2024-01-01 00:00:01', 3,
     'unused payload large enough to remain outside the projection'),
    ('beta', '1969-12-31 23:58:59', '2024-01-01 00:00:02', 5,
     'unused payload large enough to remain outside the projection'),
    ('alpha', '1970-01-01 00:01:01', '2024-01-01 00:00:03', 7,
     'unused payload large enough to remain outside the projection'),
    ('beta', '1970-01-01 00:00:00', '2024-01-01 00:00:04', 11,
     'unused payload large enough to remain outside the projection');

INSERT INTO dirty_marker_source (series, unrelated_ts, amount, payload)
VALUES ('gamma', '2024-01-01 00:00:05', 13,
        'omitted event time uses its constant default');

-- A flush runs one bounded query; merging across gaps can leave work for the next flush.
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('dirty_marker_flow');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('dirty_marker_flow');

SELECT series, window_start, row_count, amount_sum
FROM dirty_marker_sink
ORDER BY window_start, series;

-- The persisted source rows, including the defaulted time index, are unchanged by aggregation.
SELECT series, event_ts, unrelated_ts, amount
FROM dirty_marker_source
ORDER BY event_ts, series;

INSERT INTO dirty_marker_source
    (series, event_ts, unrelated_ts, amount, payload)
VALUES
    ('alpha', '1969-12-31 23:59:30', '2024-01-02 00:00:00', 4,
     'late row for an already aggregated window'),
    ('alpha', '1970-01-01 00:00:01', '2024-01-02 00:00:01', 20,
     'upsert of the existing primary key and time index');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('dirty_marker_flow');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('dirty_marker_flow');

SELECT series, window_start, row_count, amount_sum
FROM dirty_marker_sink
ORDER BY window_start, series;

SELECT series, event_ts, unrelated_ts, amount
FROM dirty_marker_source
ORDER BY event_ts, series;

DROP FLOW dirty_marker_flow;
DROP TABLE dirty_marker_sink;
DROP TABLE dirty_marker_source;
