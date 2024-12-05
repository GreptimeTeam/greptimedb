CREATE TABLE test_ttl(ts TIMESTAMP TIME INDEX, val INT) WITH (ttl = 'instant');

SHOW CREATE TABLE test_ttl;

INSERT INTO
       test_ttl
VALUES
       (now(), 1),
       (now(), 2),
       (now(), 3),
       (now() + INTERVAL '1 SECOND', 4),
       (now() - INTERVAL '1 SECOND', 5);

SELECT
       val
from
       test_ttl;

INSERT INTO
       test_ttl
VALUES
       (now(), 1),
       (now(), 2),
       (now(), 3),
       (now() + INTERVAL '1 SECOND', 4),
       (now() - INTERVAL '1 SECOND', 5);

-- SQLNESS SLEEP 2s
ADMIN flush_table('test_ttl');

ADMIN compact_table('test_ttl');

SELECT
       val
from
       test_ttl;

DROP TABLE test_ttl;