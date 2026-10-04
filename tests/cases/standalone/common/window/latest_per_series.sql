CREATE TABLE netflow_raw (
    greptime_timestamp TIMESTAMP TIME INDEX,
    src STRING,
    dst_port INT,
    dst_addr STRING,
    byte_count DOUBLE,
    flow_id INT,
    PRIMARY KEY (src, dst_port)
);

INSERT INTO netflow_raw VALUES
    (1000, 'source-a', 80, 'target-80-old', 100.0, 1),
    (5000, 'source-a', 80, 'target-80-new', 500.0, 2),
    (2000, 'source-a', 443, 'target-443-old', 200.0, 3),
    (7000, 'source-a', 443, 'target-443-new', 700.0, 4),
    (3000, 'source-a', 22, 'target-22-only', 300.0, 5),
    (4000, 'source-a', 8080, 'target-8080-only', 400.0, 6),
    (8000, 'source-b', 9999, 'target-other-src', 999.0, 7);

-- Latest row per dst_port using a window subquery. The window result must be
-- filtered outside the subquery, not directly in the WHERE clause.
SELECT greptime_timestamp, dst_port, dst_addr, byte_count, flow_id
FROM (
    SELECT
        greptime_timestamp,
        dst_port,
        dst_addr,
        byte_count,
        flow_id,
        ROW_NUMBER() OVER (
            PARTITION BY dst_port
            ORDER BY greptime_timestamp DESC
        ) AS rn
    FROM netflow_raw
    WHERE src = 'source-a'
      AND greptime_timestamp >= '1970-01-01T00:00:02'::timestamp
) latest_per_port
WHERE rn = 1
ORDER BY greptime_timestamp DESC, dst_port
LIMIT 3;

-- DISTINCT ON keeps the first row per dst_port according to its own ORDER BY.
-- Wrap it to apply the final "latest groups first" ordering and LIMIT.
SELECT greptime_timestamp, dst_port, dst_addr, byte_count, flow_id
FROM (
    SELECT DISTINCT ON (dst_port)
        greptime_timestamp,
        dst_port,
        dst_addr,
        byte_count,
        flow_id
    FROM netflow_raw
    WHERE src = 'source-a'
      AND greptime_timestamp >= '1970-01-01T00:00:02'::timestamp
    ORDER BY dst_port, greptime_timestamp DESC
) latest_per_port
ORDER BY greptime_timestamp DESC, dst_port
LIMIT 3;

-- RANK returns all rows tied at the latest timestamp for each dst_port.
SELECT greptime_timestamp, dst_port, dst_addr, byte_count, flow_id, r
FROM (
    SELECT
        greptime_timestamp,
        dst_port,
        dst_addr,
        byte_count,
        flow_id,
        RANK() OVER (
            PARTITION BY dst_port
            ORDER BY greptime_timestamp DESC
        ) AS r
    FROM netflow_raw
    WHERE src = 'source-a'
      AND greptime_timestamp >= '1970-01-01T00:00:02'::timestamp
) latest_per_port
WHERE r = 1
ORDER BY greptime_timestamp DESC, dst_port
LIMIT 3;

-- Ordered aggregate variant when the caller can enumerate the columns to fetch.
SELECT
    dst_port,
    last_value(greptime_timestamp ORDER BY greptime_timestamp) AS latest_ts,
    last_value(dst_addr ORDER BY greptime_timestamp) AS latest_dst_addr,
    last_value(byte_count ORDER BY greptime_timestamp) AS latest_byte_count,
    last_value(flow_id ORDER BY greptime_timestamp) AS latest_flow_id
FROM netflow_raw
WHERE src = 'source-a'
  AND greptime_timestamp >= '1970-01-01T00:00:02'::timestamp
GROUP BY dst_port
ORDER BY latest_ts DESC, dst_port
LIMIT 3;

CREATE TABLE netflow_raw_ties (
    greptime_timestamp TIMESTAMP TIME INDEX,
    src STRING,
    dst_port INT,
    dst_addr STRING,
    byte_count DOUBLE,
    flow_id INT,
    PRIMARY KEY (src, dst_port, flow_id)
);

INSERT INTO netflow_raw_ties VALUES
    (1000, 'source-a', 443, 'target-443-old', 100.0, 1),
    (9000, 'source-a', 443, 'target-443-tie-a', 900.0, 2),
    (9000, 'source-a', 443, 'target-443-tie-b', 901.0, 3),
    (8000, 'source-a', 80, 'target-80-new', 800.0, 4),
    (9500, 'source-b', 443, 'target-other-src', 950.0, 5);

-- RANK with r = 1 keeps all rows tied at the latest timestamp per dst_port.
SELECT greptime_timestamp, dst_port, dst_addr, byte_count, flow_id, r
FROM (
    SELECT
        greptime_timestamp,
        dst_port,
        dst_addr,
        byte_count,
        flow_id,
        RANK() OVER (
            PARTITION BY dst_port
            ORDER BY greptime_timestamp DESC
        ) AS r
    FROM netflow_raw_ties
    WHERE src = 'source-a'
      AND greptime_timestamp >= '1970-01-01T00:00:02'::timestamp
) latest_per_port
WHERE r = 1
ORDER BY greptime_timestamp DESC, dst_port, flow_id;

DROP TABLE netflow_raw_ties;

DROP TABLE netflow_raw;
