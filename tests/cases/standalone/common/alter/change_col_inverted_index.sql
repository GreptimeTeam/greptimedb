CREATE TABLE fox (
    ts TIMESTAMP TIME INDEX,
    fox STRING,
);

INSERT INTO fox VALUES
    (1, 'The quick brown fox jumps over the lazy dog'),
    (2, 'The             fox jumps over the lazy dog'),
    (3, 'The quick brown     jumps over the lazy dog'),
    (4, 'The quick brown fox       over the lazy dog'),
    (5, 'The quick brown fox jumps      the lazy dog'),
    (6, 'The quick brown fox jumps over          dog'),
    (7, 'The quick brown fox jumps over the      dog');


ALTER TABLE fox MODIFY COLUMN fox SET INVERTED INDEX;

SELECT fox FROM fox WHERE MATCHES(fox, '"fox jumps"') ORDER BY ts;

SHOW CREATE TABLE fox;

-- SQLNESS ARG restart=true
SHOW CREATE TABLE fox;

SHOW INDEX FROM fox;

ALTER TABLE fox MODIFY COLUMN fox UNSET INVERTED INDEX;

SHOW CREATE TABLE fox;

-- SQLNESS ARG restart=true
SHOW CREATE TABLE fox;

SHOW INDEX FROM fox;

DROP TABLE fox;

CREATE TABLE test_pk (ts TIMESTAMP TIME INDEX, foo STRING, bar INT, PRIMARY KEY (foo, bar));

SHOW INDEX FROM test_pk;

ALTER TABLE test_pk MODIFY COLUMN foo UNSET INVERTED INDEX;

SHOW INDEX FROM test_pk;

ALTER TABLE test_pk MODIFY COLUMN bar UNSET INVERTED INDEX;

SHOW INDEX FROM test_pk;

ALTER TABLE test_pk MODIFY COLUMN foo SET INVERTED INDEX;

SHOW INDEX FROM test_pk;

DROP TABLE test_pk;

-- The old SST has an inverted index, but not on host.
CREATE TABLE missing_inverted_index (
    ts TIMESTAMP TIME INDEX,
    indexed_col STRING INVERTED INDEX,
    host STRING
) WITH ('append_mode' = 'true');

INSERT INTO missing_inverted_index VALUES
    (1, 'a', 'server1'),
    (2, 'b', 'server2');

ADMIN FLUSH_TABLE('missing_inverted_index');

SELECT * FROM missing_inverted_index WHERE host = 'server1' ORDER BY ts;

ALTER TABLE missing_inverted_index MODIFY COLUMN host SET INVERTED INDEX;

SELECT * FROM missing_inverted_index WHERE host = 'server1' ORDER BY ts;
SELECT * FROM missing_inverted_index WHERE host = 'missing' ORDER BY ts;
SELECT * FROM missing_inverted_index WHERE indexed_col = 'a' AND host = 'server1' ORDER BY ts;
SELECT * FROM missing_inverted_index WHERE indexed_col = 'b' AND host = 'server1' ORDER BY ts;

-- New SSTs have both indexes; queries must also retain rows from the old SST.
INSERT INTO missing_inverted_index VALUES
    (3, 'a', 'server1'),
    (4, 'b', 'server2');

ADMIN FLUSH_TABLE('missing_inverted_index');

SELECT * FROM missing_inverted_index WHERE host = 'server1' ORDER BY ts;
SELECT * FROM missing_inverted_index WHERE indexed_col = 'a' AND host = 'server1' ORDER BY ts;

-- SQLNESS ARG restart=true
SELECT * FROM missing_inverted_index WHERE host = 'server1' ORDER BY ts;

-- A column absent from old SSTs can still match its default value.
ALTER TABLE missing_inverted_index ADD COLUMN source STRING DEFAULT 'legacy';
ALTER TABLE missing_inverted_index MODIFY COLUMN source SET INVERTED INDEX;

SELECT * FROM missing_inverted_index WHERE source = 'legacy' ORDER BY ts;
SELECT * FROM missing_inverted_index WHERE source = 'missing' ORDER BY ts;
SELECT * FROM missing_inverted_index WHERE indexed_col = 'a' AND source = 'legacy' ORDER BY ts;

DROP TABLE missing_inverted_index;
