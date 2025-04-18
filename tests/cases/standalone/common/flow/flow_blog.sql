-- blog usecase
CREATE TABLE velocity (
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    left_wheel FLOAT,
    right_wheel FLOAT,
    TIME INDEX(ts)
);

CREATE TABLE avg_speed (
    avg_speed DOUBLE,
    start_window TIMESTAMP TIME INDEX,
    end_window TIMESTAMP,
    update_at TIMESTAMP,
);

CREATE FLOW calc_avg_speed SINK TO avg_speed AS
SELECT
    avg((left_wheel + right_wheel) / 2) as avg_speed,
    date_bin(INTERVAL '5 second', ts) as start_window,
    date_bin(INTERVAL '5 second', ts) + INTERVAL '5 second' as end_window,
FROM
    velocity
WHERE
    left_wheel > 0.5
    AND right_wheel > 0.5
    AND left_wheel < 60
    AND right_wheel < 60
GROUP BY
    start_window;

INSERT INTO
    velocity
VALUES
    ("2021-07-01 00:00:00.200", 0.0, 0.7),
    ("2021-07-01 00:00:00.200", 0.0, 61.0),
    ("2021-07-01 00:00:02.500", 2.0, 1.0,);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_avg_speed');

SELECT
    avg_speed,
    start_window
FROM
    avg_speed;

INSERT INTO
    velocity
VALUES
    ("2021-07-01 00:00:05.100", 5.0, 4.0),
    ("2021-07-01 00:00:09.600", 2.3, 2.1);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_avg_speed');

SELECT
    avg_speed,
    start_window
FROM
    avg_speed;

DROP FLOW calc_avg_speed;

DROP TABLE velocity;

DROP TABLE avg_speed;
