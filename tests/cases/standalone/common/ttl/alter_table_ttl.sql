CREATE TABLE test_ttl(ts TIMESTAMP TIME INDEX, val INT, PRIMARY KEY(val)) WITH (ttl = '1 day');

INSERT INTO test_ttl VALUES
       (now(), 1),
       (now(), 2),
       (now(), 3);

SELECT val from test_ttl;

-- SQLNESS SLEEP 2s
ADMIN flush_table('test_ttl');

ADMIN compact_table('test_ttl');

SELECT val from test_ttl;

ALTER TABLE test_ttl SET ttl = '1 second';

-- SQLNESS SLEEP 2s
ADMIN compact_table('test_ttl');

SELECT val from test_ttl;

ALTER TABLE test_ttl SET ttl = '1 minute';

INSERT INTO test_ttl VALUES
       (now(), 1),
       (now(), 2),
       (now(), 3);

-- SQLNESS SLEEP 2s
ADMIN flush_table('test_ttl');

ADMIN compact_table('test_ttl');


SELECT val from test_ttl;


DROP TABLE test_ttl;
