CREATE TABLE base (
    desc_str STRING,
    ts TIMESTAMP TIME INDEX
);

CREATE TABLE sink (
    desc_str STRING,
    ts TIMESTAMP TIME INDEX
);

CREATE FLOW filter_out
SINK TO sink
AS
SELECT desc_str, ts FROM base
WHERE desc_str IN ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j');

SELECT options FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name = 'filter_out';

INSERT INTO base VALUES
('a', '2023-01-01 00:00:00'),
('j', '2023-01-01 00:00:09'),
('l', '2023-01-01 00:00:08');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('filter_out');

SELECT * FROM sink ORDER BY ts;

DROP FLOW filter_out;
DROP TABLE base;
DROP TABLE sink;

CREATE TABLE base (
    desc_str STRING,
    ts TIMESTAMP TIME INDEX
);

CREATE TABLE sink (
    desc_str STRING,
    ts TIMESTAMP TIME INDEX
);

CREATE FLOW filter_out
SINK TO sink
AS
SELECT desc_str, ts FROM base
WHERE desc_str NOT IN ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j');

SELECT options FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name = 'filter_out';

INSERT INTO base VALUES
('a', '2023-01-01 00:00:00'),
('j', '2023-01-01 00:00:09'),
('l', '2023-01-01 00:00:08');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('filter_out');

SELECT * FROM sink ORDER BY ts;

DROP FLOW filter_out;
DROP TABLE base;
DROP TABLE sink;

