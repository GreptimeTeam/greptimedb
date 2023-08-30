CREATE TABLE test_alt_table(i INTEGER, j TIMESTAMP TIME INDEX);

DESC TABLE test_alt_table;

ALTER TABLE test_alt_table ADD COLUMN k INTEGER;

DESC TABLE test_alt_table;

-- SQLNESS ARG restart=true
ALTER TABLE test_alt_table ADD COLUMN m INTEGER;

DESC TABLE test_alt_table;

DROP TABLE test_alt_table;
