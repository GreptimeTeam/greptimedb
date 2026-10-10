-- Per-query hints (`-- SQLNESS HINT k=v ...`) are forwarded to the server
-- over the gRPC `x-greptime-hints` header and apply to the next statement only.
--
-- The same EXPLAIN runs with the default parallelism (4) first, then with
-- `query_parallelism=2` (alone and with remote dynamic filter pushdown off),
-- then with the default again. The hint statements must plan with
-- `RoundRobinBatch(2)`: if a hint were dropped, parallelism would fall back
-- to 4 and this snapshot diff would fail.

CREATE TABLE sc_t (
    ts TIMESTAMP(3) TIME INDEX,
    v INT,
    PRIMARY KEY (v)
) ENGINE = mito;

CREATE TABLE sc_s (
    ts TIMESTAMP(3) TIME INDEX,
    v INT
) ENGINE = mito;

INSERT INTO sc_t VALUES
    ('2024-01-30 00:00:00', 1),
    ('2024-01-30 01:00:00', 6),
    ('2024-01-30 02:00:00', 10);

INSERT INTO sc_s VALUES
    ('2024-01-30 00:00:00', 5),
    ('2024-01-30 01:00:00', NULL);

ADMIN FLUSH_TABLE('sc_t');
ADMIN FLUSH_TABLE('sc_s');

-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT v FROM sc_t WHERE v > ANY(SELECT v FROM sc_s) ORDER BY v;

-- SQLNESS HINT query_parallelism=2
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT v FROM sc_t WHERE v > ANY(SELECT v FROM sc_s) ORDER BY v;

-- SQLNESS HINT query_parallelism=2 query.enable_remote_dynamic_filter_pushdown=false
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT v FROM sc_t WHERE v > ANY(SELECT v FROM sc_s) ORDER BY v;

-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT v FROM sc_t WHERE v > ANY(SELECT v FROM sc_s) ORDER BY v;

SELECT v FROM sc_t WHERE v > ANY(SELECT v FROM sc_s) ORDER BY v;

-- SQLNESS PROTOCOL MYSQL
-- SQLNESS HINT query_parallelism=2
SELECT 1;

-- SQLNESS PROTOCOL POSTGRES
-- SQLNESS HINT query_parallelism=2
SELECT 1;

DROP TABLE sc_t;
DROP TABLE sc_s;
