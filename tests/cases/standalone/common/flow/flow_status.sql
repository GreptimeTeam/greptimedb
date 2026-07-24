CREATE TABLE numbers_input_flow_status (
    "number" INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_flow_status SINK TO out_flow_status AS
SELECT
    sum(number),
    date_bin(INTERVAL '1 second', ts, '2021-07-01 00:00:00') as time_window
FROM
    numbers_input_flow_status
GROUP BY
    time_window;

INSERT INTO
    numbers_input_flow_status
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_flow_status');

-- flow_name is deterministic; the rest of the columns are runtime dependent.
SELECT flow_name FROM information_schema.flow_statistics WHERE flow_name = 'test_flow_status';

-- the like filter matches against flow_name; no matching flow returns empty.
SHOW FLOW STATUS LIKE 'no_such_flow';

DROP FLOW test_flow_status;

DROP TABLE numbers_input_flow_status;

DROP TABLE out_flow_status;
