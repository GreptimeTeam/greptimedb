-- description: Test scanning many big varchar strings with limited memory

CREATE TABLE test (a VARCHAR, ts timestamp_s time index);

-- create a big varchar (10K characters)
INSERT INTO test VALUES ('aaaaaaaaaa', 1);

-- sizes: 10, 100, 1000, 10000
INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a, to_unixtime(ts) * 3 FROM test WHERE LENGTH(a)=(SELECT MAX(LENGTH(a)) FROM test);

INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a, to_unixtime(ts) * 5 FROM test WHERE LENGTH(a)=(SELECT MAX(LENGTH(a)) FROM test);

INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a, to_unixtime(ts) * 7 FROM test WHERE LENGTH(a)=(SELECT MAX(LENGTH(a)) FROM test);

-- now create a second table, we only insert the big varchar string in there
CREATE TABLE bigtable (a VARCHAR, ts timestamp_s time index);

INSERT INTO bigtable SELECT a, ts FROM test WHERE LENGTH(a)=(SELECT MAX(LENGTH(a)) FROM test);

-- verify that the append worked
SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

-- we create a total of 16K entries in the big table
-- the total size of this table is 16K*10K = 160MB
-- we then scan the table at every step, as our buffer pool is limited to 100MB not all strings fit in memory
INSERT INTO bigtable SELECT a, to_unixtime(ts) * 11 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 23 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 31 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 37 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 41 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 47 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 51 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 53 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 57 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 61 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 63 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;


INSERT INTO bigtable SELECT a, to_unixtime(ts) * 67 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 71 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

-- SQLNESS ARG restart=true
SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 73 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;

INSERT INTO bigtable SELECT a, to_unixtime(ts) * 79 FROM bigtable;

SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable;


DROP TABLE test;

DROP TABLE bigtable;
