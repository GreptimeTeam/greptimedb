CREATE TABLE test_alt_table(h INTEGER, i INTEGER, j TIMESTAMP TIME INDEX, PRIMARY KEY (h, i));

DESC TABLE test_alt_table;

INSERT INTO test_alt_table VALUES (1, 1, 0), (2, 2, 1);

-- TODO: It may result in an error if `k` is with type INTEGER.
-- Error: 3001(EngineExecuteQuery), Invalid argument error: column types must match schema types, expected Int32 but found Utf8 at column index 3
ALTER TABLE test_alt_table ADD COLUMN k STRING PRIMARY KEY;

DESC TABLE test_alt_table;

SELECT * FROM test_alt_table;

SELECT * FROM test_alt_table WHERE i = 1;

-- SQLNESS ARG restart=true
ALTER TABLE test_alt_table ADD COLUMN m INTEGER;

ALTER TABLE test_alt_table ADD COLUMN dt DATETIME;

-- Should fail issue #5422
ALTER TABLE test_alt_table ADD COLUMN n interval;

-- Should fail issue #5422
ALTER TABLE test_alt_table MODIFY COLUMN m interval;

INSERT INTO test_alt_table (h, i, j, m, dt) VALUES (42, 42, 0, 11, 0);

ALTER TABLE test_alt_table MODIFY COLUMN m Float64;

SELECT * FROM test_alt_table;

ALTER TABLE test_alt_table MODIFY COLUMN m INTEGER;

SELECT * FROM test_alt_table;

ALTER TABLE test_alt_table MODIFY COLUMN m BOOLEAN;

SELECT * FROM test_alt_table;

DESC TABLE test_alt_table;

DROP TABLE test_alt_table;

-- test if column with default value can change type properly
CREATE TABLE test_alt_table_default(h INTEGER, i INTEGER DEFAULT 0, j TIMESTAMP TIME INDEX, PRIMARY KEY (h));

ALTER TABLE test_alt_table_default MODIFY COLUMN i BOOLEAN;

INSERT INTO test_alt_table_default (h, j) VALUES (1, 0), (2, 1);

SELECT * FROM test_alt_table_default ORDER BY h;

ALTER TABLE test_alt_table_default MODIFY COLUMN i INTEGER;

DESC TABLE test_alt_table_default;

INSERT INTO test_alt_table_default (h, j) VALUES (3, 0), (4, 1);

SELECT * FROM test_alt_table_default ORDER BY h;

ALTER TABLE test_alt_table_default MODIFY COLUMN i STRING;

INSERT INTO test_alt_table_default (h, j) VALUES (5, 0);

INSERT INTO test_alt_table_default (h, i, j) VALUES (6, "word" ,1);

SELECT * FROM test_alt_table_default ORDER BY h;

DROP TABLE test_alt_table_default;

-- to test if same name column can be added
CREATE TABLE phy (ts timestamp time index, val double) engine = metric with ("physical_metric_table" = "");

CREATE TABLE t1 (
    ts timestamp time index,
    val double,
    host string primary key
) engine = metric with ("on_physical_table" = "phy");

INSERT INTO
    t1
VALUES
    ('host1', 0, 1),
    ('host2', 1, 0,);

SELECT
    *
FROM
    t1;

CREATE TABLE t2 (
    ts timestamp time index,
    job string primary key,
    val double
) engine = metric with ("on_physical_table" = "phy");

ALTER TABLE
    t1
ADD
    COLUMN `at` STRING;

ALTER TABLE
    t2
ADD
    COLUMN at3 STRING;

ALTER TABLE
    t2
ADD
    COLUMN `at` STRING;

ALTER TABLE
    t2
ADD
    COLUMN at2 STRING;

ALTER TABLE
    t2
ADD
    COLUMN at4 UINT16;

INSERT INTO
    t2
VALUES
    ("loc_1", "loc_2", "loc_3", 2, 'job1', 0, 1);

SELECT
    *
FROM
    t2;

DROP TABLE t1;

DROP TABLE t2;

DROP TABLE phy;

CREATE TABLE grpc_latencies (
  ts TIMESTAMP TIME INDEX,
  host STRING,
  method_name STRING,
  latency DOUBLE,
  PRIMARY KEY (host, method_name)
) with('append_mode'='true');

INSERT INTO grpc_latencies (ts, host, method_name, latency) VALUES
  ('2024-07-11 20:00:06', 'host1', 'GetUser', 103.0);

SELECT * FROM grpc_latencies;

ALTER TABLE grpc_latencies SET ttl = '10000d';

ALTER TABLE grpc_latencies ADD COLUMN home INTEGER FIRST;

SELECT * FROM grpc_latencies;

DROP TABLE grpc_latencies;
