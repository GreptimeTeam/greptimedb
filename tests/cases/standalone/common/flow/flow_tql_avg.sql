CREATE TABLE sensor_readings (
    `value` DOUBLE,
    ts TIMESTAMP TIME INDEX,
    sensor STRING,
    loc STRING,
    PRIMARY KEY (sensor, loc)
);

CREATE TABLE sensor_readings_avg (
    `value` DOUBLE,
    ts TIMESTAMP TIME INDEX,
    sensor STRING,
    PRIMARY KEY (sensor)
);

INSERT INTO sensor_readings VALUES
    (20, now() - '30s'::interval, 'test', 'A');

-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}) TS
TQL EVAL (now() - '1m'::interval, now(), '1m')
avg by(sensor) (sensor_readings) AS value;

-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}) TS
TQL EVAL (now() - '1m'::interval, now(), '1m') (sum by(sensor) (sensor_readings) / count by(sensor) (sensor_readings)) AS value;

CREATE FLOW sensor_readings_avg_flow
SINK TO sensor_readings_avg
EVAL INTERVAL '1m' AS
TQL EVAL (now() - '1m'::interval, now(), '1m')
avg by(sensor) (sensor_readings) AS value;

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('sensor_readings_avg_flow');

-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}) TS
SELECT * FROM sensor_readings_avg ORDER BY ts DESC LIMIT 1;

DROP FLOW sensor_readings_avg_flow;

-- SQLNESS SLEEP 1s
INSERT INTO sensor_readings VALUES
    (30, now() - '40s'::interval, 'test', 'B');

-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}) TS
TQL EVAL (now() - '1m'::interval, now(), '1m')
avg by(sensor) (sensor_readings) AS value;


CREATE FLOW sensor_readings_avg_flow
SINK TO sensor_readings_avg
EVAL INTERVAL '1m' AS
TQL EVAL (now() - '1m'::interval, now(), '1m') (sum by(sensor) (sensor_readings) / count by(sensor) (sensor_readings)) AS value;

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('sensor_readings_avg_flow');

-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}) TS
SELECT * FROM sensor_readings_avg ORDER BY ts DESC LIMIT 1;

DROP FLOW sensor_readings_avg_flow;

DROP TABLE sensor_readings_avg;
DROP TABLE sensor_readings;
