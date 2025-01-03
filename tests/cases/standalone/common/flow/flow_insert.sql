-- test if reordered insert is correctly handled
CREATE TABLE bytes_log (
    byte INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- event time
    TIME INDEX(ts)
);

-- TODO(discord9): remove this after auto infer table's time index is impl
CREATE TABLE approx_rate (
    rate DOUBLE,
    time_window TIMESTAMP,
    update_at TIMESTAMP,
    TIME INDEX(time_window)
);

CREATE FLOW find_approx_rate SINK TO approx_rate AS
SELECT
    (max(byte) - min(byte)) / 30.0 as rate,
    date_bin(INTERVAL '30 second', ts) as time_window
from
    bytes_log
GROUP BY
    time_window;

SHOW CREATE TABLE approx_rate;

-- reordered insert, also test if null is handled correctly
INSERT INTO
    bytes_log (ts, byte) 
VALUES
    ('2023-01-01 00:00:01', NULL),
    ('2023-01-01 00:00:29', 300);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('find_approx_rate');

SELECT
    rate,
    time_window
FROM
    approx_rate;

-- reordered insert, also test if null is handled correctly
INSERT INTO
    bytes_log (ts, byte) 
VALUES
    ('2022-01-01 00:00:01', NULL),
    ('2022-01-01 00:00:29', NULL);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('find_approx_rate');

SELECT
    rate,
    time_window
FROM
    approx_rate;

-- reordered insert
INSERT INTO
    bytes_log (ts, byte) 
VALUES
    ('2025-01-01 00:00:01', 101),
    ('2025-01-01 00:00:29', 300);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('find_approx_rate');

SELECT
    rate,
    time_window
FROM
    approx_rate;

-- reordered insert
INSERT INTO
    bytes_log (ts, byte) 
VALUES
    ('2025-01-01 00:00:32', 450),
    ('2025-01-01 00:00:37', 500);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('find_approx_rate');

SELECT
    rate,
    time_window
FROM
    approx_rate;

DROP TABLE bytes_log;

DROP FLOW find_approx_rate;

DROP TABLE approx_rate;
