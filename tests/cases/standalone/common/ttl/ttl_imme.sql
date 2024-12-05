CREATE TABLE test_ttl(ts TIMESTAMP TIME INDEX, val INT) WITH (ttl = 'instant');

SHOW CREATE TABLE test_ttl;

INSERT INTO test_ttl VALUES
       (now(), 1);

SELECT val from test_ttl;

-- SQLNESS SLEEP 2s
ADMIN flush_table('test_ttl');

ADMIN compact_table('test_ttl');

SELECT val from test_ttl;

DROP TABLE test_ttl;
