CREATE TABLE test_add_col(i INTEGER, j BIGINT TIME INDEX);

INSERT INTO test_add_col VALUES (1, 1), (2, 2);

ALTER TABLE test_add_col ADD COLUMN k INTEGER;

SELECT * FROM test_add_col;

DROP TABLE test_add_col;
