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
