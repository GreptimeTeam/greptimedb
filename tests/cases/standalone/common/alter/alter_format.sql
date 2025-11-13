CREATE TABLE test_alt_format(h INTEGER, i INTEGER DEFAULT 0, j TIMESTAMP TIME INDEX, PRIMARY KEY (h)) WITH ('sst_format' = 'primary_key');

ALTER TABLE test_alt_format SET 'sst_format' = 'primary_key';

INSERT INTO test_alt_format (h, j) VALUES (10, 0);

ALTER TABLE test_alt_format ADD COLUMN k INTEGER;

INSERT INTO test_alt_format (h, j) VALUES (11, 1);

-- SQLNESS SORT_RESULT 3 1
SELECT * FROM test_alt_format;

-- SQLNESS SORT_RESULT 3 1
SELECT i, h FROM test_alt_format;

ALTER TABLE test_alt_format SET 'sst_format' = 'flat';

-- SQLNESS SORT_RESULT 3 1
SELECT * FROM test_alt_format;

INSERT INTO test_alt_format (h, j) VALUES (12, 2);

INSERT INTO test_alt_format (h, j, i, k) VALUES (13, 3, 23, 33);

-- SQLNESS SORT_RESULT 3 1
SELECT * FROM test_alt_format;

-- SQLNESS SORT_RESULT 3 1
SELECT i, h FROM test_alt_format;

ADMIN flush_table('test_alt_format');

-- SQLNESS SORT_RESULT 3 1
SELECT * FROM test_alt_format;

-- SQLNESS SORT_RESULT 3 1
SELECT i, h FROM test_alt_format;

-- not allow to change from flat to primary_key
-- SQLNESS REPLACE \d+\(\d+,\s+\d+\) REDACTED
ALTER TABLE test_alt_format SET 'sst_format' = 'primary_key';

DROP TABLE test_alt_format;

CREATE TABLE alt_format_phy (ts timestamp time index, val double) engine=metric with ("physical_metric_table" = "", "sst_format" = "primary_key");

CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "alt_format_phy");

INSERT INTO t1 (ts, val, host) VALUES
  ('2022-01-01 00:00:00', 1.23, 'example.com'),
  ('2022-01-01 00:00:00', 1.23, 'hello.com'),
  ('2022-01-02 00:00:00', 4.56, 'example.com');

ALTER TABLE alt_format_phy SET 'sst_format' = 'primary_key';

ALTER TABLE t1 SET 'sst_format' = 'primary_key';

ALTER TABLE t1 ADD COLUMN k STRING PRIMARY KEY;

SELECT * FROM t1 ORDER BY ts ASC;

ALTER TABLE alt_format_phy SET 'sst_format' = 'flat';

SELECT * FROM t1 ORDER BY ts ASC;

SELECT host, ts, val FROM t1 where host = 'example.com' ORDER BY ts ASC;

INSERT INTO t1 (ts, val, host) VALUES
  ('2022-01-01 00:00:01', 3.0, 'example.com'),
  ('2022-01-01 00:00:01', 4.0, 'hello.com');

SELECT host, ts, val FROM t1 where host = 'example.com' ORDER BY ts ASC;

-- not allow to change from flat to primary_key
-- SQLNESS REPLACE \d+\(\d+,\s+\d+\) REDACTED
ALTER TABLE alt_format_phy SET 'sst_format' = 'primary_key';

DROP TABLE t1;

DROP TABLE alt_format_phy;
