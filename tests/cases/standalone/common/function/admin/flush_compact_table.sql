--- test flush_table and compact_table ---

CREATE TABLE test(ts timestamp time index);

INSERT INTO test VALUES (1), (2), (3), (4), (5);

SELECT * FROM test;

ADMIN FLUSH_TABLE('test');

ADMIN COMPACT_TABLE('test');

ADMIN COMPACT_TABLE(
    'test',
    'regular',
    'start_time=1970-01-01T00:00:00Z,end_time=1970-01-01T01:00:00Z'
);

ADMIN COMPACT_TABLE(
    'test',
    'strict_window',
    'window=3600,start_time=1970-01-01T00:00:00Z,end_time=1970-01-01T01:00:00Z'
);

SELECT FLUSH_TABLE('test');

SELECT COMPACT_TABLE('test');

--- doesn't change anything ---
SELECT * FROM test;

DROP TABLE test;
