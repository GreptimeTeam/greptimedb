-- description: Insert big varchar strings

CREATE TABLE test (a VARCHAR, ts timestamp time index);

-- insert a big varchar
INSERT INTO test VALUES ('aaaaaaaaaa', 1);

-- sizes: 10, 100, 1000, 10000
INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a, 2 FROM test WHERE LENGTH(a)=(SELECT MAX(LENGTH(a)) FROM test);

INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a, 3 FROM test WHERE LENGTH(a)=(SELECT MAX(LENGTH(a)) FROM test);

INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a, 4 FROM test WHERE LENGTH(a)=(SELECT MAX(LENGTH(a)) FROM test);

SELECT LENGTH(a) FROM test ORDER BY 1;

DROP TABLE test;
