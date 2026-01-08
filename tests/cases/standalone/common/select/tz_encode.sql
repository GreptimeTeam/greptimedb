
CREATE TABLE ngx_access_log (
    client STRING,
    country STRING,
    access_time TIMESTAMP(9) TIME INDEX
);

/* insert some data */
INSERT INTO
    ngx_access_log
VALUES
    ("client1", "US", "2022-01-01 00:00:00"),
    ("client2", "US", "2022-01-01 00:00:01"),
    ("client3", "UK", "2022-01-01 00:00:02"),
    ("client4", "UK", "2022-01-01 00:00:03"),
    ("client5", "CN", "2022-01-01 00:00:04"),
    ("client6", "CN", "2022-01-01 00:00:05"),
    ("client7", "JP", "2022-01-01 00:00:06"),
    ("client8", "JP", "2022-01-01 00:00:07"),
    ("client9", "KR", "2022-01-01 00:00:08"),
    ("client10", "KR", "2022-01-01 00:00:09");

-- should not fail with mismatch timezone
-- SQLNESS REPLACE \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z NOW
SELECT
    now()
FROM
    ngx_access_log;

-- SQLNESS REPLACE TimestampNanosecond\(\d+ TimestampNanosecond(NOW
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
EXPLAIN SELECT
    now()
FROM
    ngx_access_log;

DROP TABLE ngx_access_log;