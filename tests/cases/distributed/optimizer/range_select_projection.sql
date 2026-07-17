CREATE TABLE range_select_projection (
    ts TIMESTAMP TIME INDEX,
    station STRING,
    "channel" STRING,
    unused_tag STRING,
    value_a DOUBLE,
    value_b DOUBLE,
    unused_value DOUBLE,
    PRIMARY KEY (station, "channel", unused_tag),
);

INSERT INTO range_select_projection VALUES
    (0, 'station-a', 'channel-a', 'unused-a', 1.0, 10.0, 100.0),
    (5000, 'station-a', 'channel-a', 'unused-a', 2.0, 20.0, 200.0),
    (10000, 'station-a', 'channel-a', 'unused-a', 3.0, 30.0, 300.0),
    (0, 'station-a', 'channel-b', 'unused-b', 4.0, 40.0, 400.0),
    (5000, 'station-a', 'channel-b', 'unused-b', 5.0, 50.0, 500.0);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT
    ts,
    station,
    "channel",
    avg(value_a + value_b) RANGE '10s' AS avg_value
FROM range_select_projection
WHERE station = 'station-a'
    AND unused_tag = 'unused-a'
    AND ts >= '1970-01-01 00:00:00'
    AND ts < '1970-01-01 00:00:15'
ALIGN '5s' BY (station, "channel")
ORDER BY station, "channel", ts;

SELECT
    ts,
    station,
    "channel",
    avg(value_a + value_b) RANGE '10s' AS avg_value
FROM range_select_projection
WHERE station = 'station-a'
    AND unused_tag = 'unused-a'
    AND ts >= '1970-01-01 00:00:00'
    AND ts < '1970-01-01 00:00:15'
ALIGN '5s' BY (station, "channel")
ORDER BY station, "channel", ts;

DROP TABLE range_select_projection;
