--- alter table to add new column with default timestamp values aware of session timezone test ---

CREATE TABLE test1 (i INTEGER, j TIMESTAMP time index, PRIMARY KEY(i));

INSERT INTO test1 values (1, 1), (2, 2);

SELECT * FROM test1;

--- add ts1 column ---
ALTER TABLE test1 ADD COLUMN ts1 TIMESTAMP DEFAULT '2024-01-30 00:01:01' PRIMARY KEY;

INSERT INTO test1 values (3, 3, DEFAULT), (4, 4,  '2024-01-31 00:01:01');

-- SQLNESS SORT_RESULT 3 1
SELECT i, ts1 FROM test1;

SET time_zone = 'Asia/Shanghai';

--- add ts2 column, default value is the same as ts1, but with different session timezone ---
ALTER TABLE test1 ADD COLUMN ts2 TIMESTAMP DEFAULT '2024-01-30 00:01:01' PRIMARY KEY;

INSERT INTO test1 values (5, 5, DEFAULT, DEFAULT), (6, 6, DEFAULT, '2024-01-31 00:01:01');

-- SQLNESS SORT_RESULT 3 1
SELECT i, ts1, ts2 FROM test1;

SET time_zone = 'UTC';

DROP TABLE test1;
