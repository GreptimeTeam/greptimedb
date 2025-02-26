CREATE DATABASE test_ttl_db WITH (ttl = '1 second');

USE test_ttl_db;

-- It will use the database TTL setting --
CREATE TABLE test_ttl(ts TIMESTAMP TIME INDEX, val INT);

INSERT INTO test_ttl VALUES
       (now(), 1);

SELECT val from test_ttl;

-- SQLNESS SLEEP 2s
ADMIN flush_table('test_ttl');

ADMIN compact_table('test_ttl');

-- Must be expired --
SELECT val from test_ttl;

ALTER DATABASE test_ttl_db SET ttl = '1 day';

INSERT INTO test_ttl VALUES
       (now(), 1);

-- SQLNESS SLEEP 2s
ADMIN flush_table('test_ttl');

ADMIN compact_table('test_ttl');

-- Must not be expired --
SELECT val from test_ttl;

DROP TABLE test_ttl;


USE public;

DROP DATABASE test_ttl_db;
