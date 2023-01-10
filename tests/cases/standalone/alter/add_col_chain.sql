CREATE TABLE test(i INTEGER, j BIGINT TIME INDEX);

INSERT INTO test VALUES (1, 1), (2, 2);

INSERT INTO test VALUES (3, 3);

ALTER TABLE test ADD COLUMN k INTEGER;

ALTER TABLE test ADD COLUMN l INTEGER;

ALTER TABLE test ADD COLUMN m INTEGER DEFAULT 3;

SELECT * FROM test;

DROP TABLE test;
