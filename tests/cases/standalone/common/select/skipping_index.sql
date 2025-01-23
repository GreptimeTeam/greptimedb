create table
    skipping_table (
        ts timestamp time index,
        `id` string skipping index,
        `name` string skipping index
        with
            (granularity = 8192),
    );

INSERT INTO skipping_table VALUES (1664356800000, 'id1', 'name1');

ADMIN FLUSH_TABLE('skipping_table');

INSERT INTO skipping_table VALUES (1664356801000, 'id2', 'name2');

ADMIN FLUSH_TABLE('skipping_table');

INSERT INTO skipping_table VALUES (1664356802000, 'id3', 'name3');

ADMIN FLUSH_TABLE('skipping_table');

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT * FROM skipping_table WHERE id = 'id2' ORDER BY `name`;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT * FROM skipping_table WHERE id = 'id5' ORDER BY `name`;

DROP TABLE IF EXISTS skipping_table;
