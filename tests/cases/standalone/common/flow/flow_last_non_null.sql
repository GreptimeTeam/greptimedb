
CREATE TABLE bytes_log (
    byte INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- event time
    TIME INDEX(ts)
);

-- TODO(discord9): remove this after auto infer table's time index is impl
CREATE TABLE approx_rate (
    rate DOUBLE NULL,
    time_window TIMESTAMP,
    update_at TIMESTAMP,
    bb DOUBLE NULL,
    TIME INDEX(time_window)
)with('merge_mode'='last_non_null');

INSERT INTO approx_rate(rate, time_window, update_at) VALUES (0.0, '2023-01-01 00:00:00', TIMESTAMP '2023-01-01 00:00:00.100');

INSERT INTO approx_rate(time_window, update_at, bb) VALUES ('2023-01-01 00:00:00', TIMESTAMP '2023-01-01 00:00:00.200', 50.0);

select * from approx_rate;

CREATE FLOW find_approx_rate SINK TO approx_rate AS
SELECT
    (max(byte) - min(byte)) / 30.0 as rate,
    date_bin(INTERVAL '30 second', ts) as time_window,
    TIMESTAMP '2023-01-01 00:00:10' as update_at
from
    bytes_log
GROUP BY
    time_window;

SHOW CREATE TABLE approx_rate;

INSERT INTO
    bytes_log
VALUES
    (NULL, '2023-01-01 00:00:01'),
    (300, '2023-01-01 00:00:31');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('find_approx_rate');

SELECT * FROM approx_rate;

CREATE FLOW find_bb_only SINK TO approx_rate AS
SELECT
    date_bin(INTERVAL '30 second', ts) as time_window,
    TIMESTAMP '2023-01-01 00:00:59' as update_at,
    CAST(max(byte) AS DOUBLE) as bb
from
    bytes_log
GROUP BY
    time_window;

-- make new windows dirty after flow is created
INSERT INTO bytes_log VALUES (600, '2023-01-01 00:00:10'), (320, '2023-01-01 00:00:35');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('find_bb_only');

SELECT * FROM approx_rate;

DROP FLOW find_approx_rate;
DROP FLOW find_bb_only;
DROP TABLE bytes_log;
DROP TABLE approx_rate;
