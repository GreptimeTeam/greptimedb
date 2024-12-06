CREATE TABLE test_ttl(
       ts TIMESTAMP TIME INDEX,
       val INT,
       PRIMARY KEY (`val`)
) WITH (ttl = 'instant');

SHOW CREATE TABLE test_ttl;

INSERT INTO
       test_ttl
VALUES
       (now(), 1),
       (now(), 2),
       (now(), 3);

SELECT
       val
from
       test_ttl
ORDER BY
       val;

-- SQLNESS SLEEP 2s
ADMIN flush_table('test_ttl');

ADMIN compact_table('test_ttl');

SELECT
       val
from
       test_ttl
ORDER BY
       val;

ALTER TABLE
       test_ttl UNSET 'ttl';

INSERT INTO
       test_ttl
VALUES
       (now(), 1),
       (now(), 2),
       (now(), 3);

SELECT
       val
from
       test_ttl
ORDER BY
       val;

DROP TABLE test_ttl;

CREATE TABLE test_ttl(
       ts TIMESTAMP TIME INDEX,
       val INT,
       PRIMARY KEY (`val`)
) WITH (ttl = '1s');

SHOW CREATE TABLE test_ttl;

INSERT INTO
       test_ttl
VALUES
       (now(), 1),
       (now(), 2),
       (now(), 3);

SELECT
       val
from
       test_ttl
ORDER BY
       val;

ADMIN flush_table('test_ttl');

ADMIN compact_table('test_ttl');

SELECT
       val
from
       test_ttl
ORDER BY
       val;

-- SQLNESS SLEEP 2s
ADMIN flush_table('test_ttl');

ADMIN compact_table('test_ttl');

SELECT
       val
from
       test_ttl
ORDER BY
       val;

ALTER TABLE
       test_ttl
SET
       ttl = '1d';

INSERT INTO
       test_ttl
VALUES
       (now(), 1),
       (now(), 2),
       (now(), 3);

SELECT
       val
from
       test_ttl
ORDER BY
       val;

ALTER TABLE
       test_ttl
SET
       ttl = 'instant';

ADMIN flush_table('test_ttl');

ADMIN compact_table('test_ttl');

SELECT
       val
from
       test_ttl
ORDER BY
       val;

-- to makesure alter back and forth from duration to/from instant wouldn't break anything
ALTER TABLE
       test_ttl
SET
       ttl = '1s';

INSERT INTO
       test_ttl
VALUES
       (now(), 1),
       (now(), 2),
       (now(), 3);

SELECT
       val
from
       test_ttl
ORDER BY
       val;

-- SQLNESS SLEEP 2s
ADMIN flush_table('test_ttl');

ADMIN compact_table('test_ttl');

SELECT
       val
from
       test_ttl
ORDER BY
       val;

DROP TABLE test_ttl;
