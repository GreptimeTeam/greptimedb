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

DESC TABLE test_alt_table;

DROP TABLE test_alt_table;

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
