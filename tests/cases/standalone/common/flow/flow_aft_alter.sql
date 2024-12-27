-- test if flow can get table schema correctly after table have benn altered

CREATE TABLE bytes_log (
    byte INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- event time
    TIME INDEX(ts)
);

CREATE TABLE approx_rate (
    rate DOUBLE,
    time_window TIMESTAMP,
    update_at TIMESTAMP,
    TIME INDEX(time_window)
);

-- make both src&sink table in cache of flownode by using them
CREATE FLOW find_approx_rate SINK TO approx_rate AS
SELECT
    (max(byte) - min(byte)) / 30.0 as rate,
    date_bin(INTERVAL '30 second', ts) as time_window
from
    bytes_log
GROUP BY
    time_window;

SHOW CREATE FLOW find_approx_rate;

DROP FLOW find_approx_rate;

ALTER TABLE bytes_log ADD COLUMN stat INT DEFAULT 200 AFTER byte;
ALTER TABLE approx_rate ADD COLUMN sample_cnt INT64 DEFAULT 0 AFTER rate;

CREATE FLOW find_approx_rate SINK TO approx_rate AS
SELECT
    (max(byte) - min(byte)) / 30.0 as rate,
    count(byte) as sample_cnt,
    date_bin(INTERVAL '30 second', ts) as time_window
from
    bytes_log
GROUP BY
    time_window;

INSERT INTO
    bytes_log
VALUES
    (0,  200, '2023-01-01 00:00:01'),
    (300,200, '2023-01-01 00:00:29');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('find_approx_rate');

SELECT
    rate,
    sample_cnt,
    time_window
FROM
    approx_rate;

DROP TABLE bytes_log;

DROP FLOW find_approx_rate;

DROP TABLE approx_rate;
