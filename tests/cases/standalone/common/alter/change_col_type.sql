CREATE TABLE test(`id` INTEGER PRIMARY KEY, i INTEGER NULL, j TIMESTAMP TIME INDEX, k BOOLEAN);

INSERT INTO test VALUES (1, 1, 1, false), (2, 2, 2, true);

ALTER TABLE test MODIFY COLUMN "I" STRING;

ALTER TABLE test MODIFY COLUMN k DATE;

ALTER TABLE test MODIFY COLUMN id STRING;

ALTER TABLE test MODIFY COLUMN j STRING;

ALTER TABLE test MODIFY COLUMN I STRING;

SELECT * FROM test;

INSERT INTO test VALUES (3, "greptime", 3, true);

-- SQLNESS SORT_RESULT 3 1
SELECT * FROM test;

DESCRIBE test;

ALTER TABLE test MODIFY COLUMN I INTEGER;

-- SQLNESS SORT_RESULT 3 1
SELECT * FROM test;

DESCRIBE test;

DROP TABLE test;

CREATE TABLE ts_widen (host STRING, ts TIMESTAMP TIME INDEX);
INSERT INTO ts_widen VALUES ("a", "2024-01-01 00:00:01");

-- widen the time index unit: millisecond -> microsecond
ALTER TABLE ts_widen MODIFY COLUMN ts TIMESTAMP_US;
DESCRIBE ts_widen;

-- historical data is interpreted in the new unit
SELECT * FROM ts_widen ORDER BY ts;

-- predicate on the widened time index reads old-unit SST data correctly
SELECT * FROM ts_widen WHERE ts > '2024-01-01 00:00:01' ORDER BY ts;

SELECT * FROM ts_widen WHERE ts < '2024-01-01 00:00:01.000300' ORDER BY ts;

-- new writes can use sub-millisecond precision
INSERT INTO ts_widen VALUES ("b", "2024-01-01 00:00:01.000500");

-- filters see both the old-unit SST row and the new-unit memtable row correctly
SELECT * FROM ts_widen WHERE ts > '2024-01-01 00:00:01.000001' ORDER BY ts;

SELECT * FROM ts_widen WHERE ts > '2024-01-01 00:00:00.999999' ORDER BY ts;
SELECT * FROM ts_widen ORDER BY ts;

-- narrowing the time index unit is rejected
ALTER TABLE ts_widen MODIFY COLUMN ts TIMESTAMP;

-- changing the time index to a non-timestamp type is rejected
ALTER TABLE ts_widen MODIFY COLUMN ts STRING;

DROP TABLE ts_widen;

-- Regression: predicates must filter old-unit SST rows exactly after the
-- widen. The fixture stores ten rows in one row group (one SST, flushed in
-- the old millisecond unit), so any predicate that matches only a subset of
-- the rows exposes an over-inclusive scan that returns whole row groups.
CREATE TABLE ts_widen_multi (host STRING, ts TIMESTAMP TIME INDEX);
INSERT INTO ts_widen_multi VALUES
  ("a", "2024-01-01 00:00:00"), ("b", "2024-01-01 00:00:01"),
  ("c", "2024-01-01 00:00:02"), ("d", "2024-01-01 00:00:03"),
  ("e", "2024-01-01 00:00:04"), ("f", "2024-01-01 00:00:05"),
  ("g", "2024-01-01 00:00:06"), ("h", "2024-01-01 00:00:07"),
  ("i", "2024-01-01 00:00:08"), ("j", "2024-01-01 00:00:09");

-- force the rows into an SST written in the old (millisecond) unit
ADMIN FLUSH_TABLE('ts_widen_multi');

ALTER TABLE ts_widen_multi MODIFY COLUMN ts TIMESTAMP_US;

-- equality on the widened time index returns exactly one old-unit row
SELECT * FROM ts_widen_multi WHERE ts = '2024-01-01 00:00:07' ORDER BY ts;

SELECT * FROM ts_widen_multi WHERE ts != '2024-01-01 00:00:07' ORDER BY ts;

-- boundaries strictly between the stored seconds
SELECT * FROM ts_widen_multi WHERE ts > '2024-01-01 00:00:06.5' ORDER BY ts;

SELECT * FROM ts_widen_multi WHERE ts <= '2024-01-01 00:00:05.0005' ORDER BY ts;

-- an IN-list literal that no millisecond row can equal matches nothing
SELECT * FROM ts_widen_multi WHERE ts IN ('2024-01-01 00:00:02', '2024-01-01 00:00:05.5', '2024-01-01 00:00:08') ORDER BY ts;

DROP TABLE ts_widen_multi;

CREATE TABLE ts_widen_part (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX)
PARTITION ON COLUMNS (ts) (
  ts < '2024-01-02 00:00:00',
  ts >= '2024-01-02 00:00:00'
);
INSERT INTO ts_widen_part VALUES ("a", 1, "2024-01-01 23:59:59.5");

-- widen the time index unit of a table partitioned on the time index
ALTER TABLE ts_widen_part MODIFY COLUMN ts TIMESTAMP_US;

-- historical data still routes/joins correctly
SELECT * FROM ts_widen_part ORDER BY ts;

-- new writes land in the right partition
INSERT INTO ts_widen_part VALUES ("b", 2, "2024-01-02 00:00:00.000250");
SELECT * FROM ts_widen_part ORDER BY ts;

DROP TABLE ts_widen_part;
