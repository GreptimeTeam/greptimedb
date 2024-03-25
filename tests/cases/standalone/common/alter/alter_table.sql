CREATE TABLE test_alt_table(i INTEGER, j TIMESTAMP TIME INDEX);

DESC TABLE test_alt_table;

INSERT INTO test_alt_table VALUES (1, 0), (2, 1);

ALTER TABLE test_alt_table ADD COLUMN k INTEGER;

DESC TABLE test_alt_table;

SELECT * FROM test_alt_table;

SELECT * FROM test_alt_table WHERE k IS NULL;

SELECT * FROM test_alt_table WHERE i = 1;

-- SQLNESS ARG restart=true
ALTER TABLE test_alt_table ADD COLUMN m INTEGER;

DESC TABLE test_alt_table;

DROP TABLE test_alt_table;
