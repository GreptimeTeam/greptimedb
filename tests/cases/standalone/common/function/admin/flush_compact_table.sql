--- test flush_table and compact_table ---

CREATE TABLE test(ts timestamp time index);

INSERT INTO test VALUES (1), (2), (3), (4), (5);

SELECT * FROM test;

ADMIN FLUSH_TABLE('test');

ADMIN COMPACT_TABLE('test');

--- doesn't change anything ---
SELECT * FROM test;

DROP TABLE test;
