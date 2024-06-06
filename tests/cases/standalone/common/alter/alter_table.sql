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

DESC TABLE test_alt_table;

DROP TABLE test_alt_table;
