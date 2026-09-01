--- insert timestamp with default values aware of session timezone test ---

CREATE TABLE test1 (i INTEGER, j TIMESTAMP default '2024-01-30 00:01:01' TIME INDEX, PRIMARY KEY(i));

INSERT INTO test1 VALUES (1, DEFAULT), (2, DEFAULT), (3, '2024-01-31 00:01:01'), (4, '2025-02-01 00:01:01');

SELECT * FROM test1 ORDER BY j;

SET time_zone = 'Asia/Shanghai';

CREATE TABLE test2 (i INTEGER, j TIMESTAMP default '2024-01-30 00:01:01' TIME INDEX, PRIMARY KEY(i));

INSERT INTO test2 VALUES (1, DEFAULT), (2, DEFAULT), (3, '2024-01-31 00:01:01'), (4, '2025-02-01 00:01:01');

SELECT * FROM test2 ORDER BY j;

SELECT * FROM test1 ORDER BY j;

CREATE TABLE test3 (ts TIMESTAMP TIME INDEX, st TIMESTAMP, ts_ns TIMESTAMP(9));

INSERT INTO test3 (ts) VALUES ('2026-08-01 12:00:00.001');

INSERT INTO test3 (ts, st) VALUES ('2026-08-02 12:00:00.001', now());

INSERT INTO test3 (ts, st) SELECT '2026-08-03 12:00:00.001', now();

INSERT INTO test3 (ts, st) SELECT '2026-08-04 12:00:00.001', now() LIMIT 1;

INSERT INTO test3 (ts, st)
SELECT '2026-08-06 12:00:00.001', now()
UNION ALL
SELECT '2026-08-07 12:00:00.001', now();

INSERT INTO test3 (ts, st) VALUES (
    CAST('2026-08-08 12:00:00.001' AS TIMESTAMP),
    now()
);

INSERT INTO test3 (ts, st)
SELECT '2026-08-10 12:00:00.001', now()
UNION ALL
SELECT CAST('2026-08-11 12:00:00.001' AS TIMESTAMP), now();

INSERT INTO test3 (ts, st)
SELECT '2026-08-16 12:00:00.001', now()
UNION
SELECT '2026-08-17 12:00:00.001', now();

INSERT INTO test3 (ts, ts_ns) SELECT a, b FROM (
    SELECT c AS a, c AS b FROM (SELECT '2026-08-12 12:00:00.123456789' AS c) AS t1
) AS t2;

INSERT INTO test3 (ts, st, ts_ns) VALUES (
    '2026-08-09 12:00:00.001',
    now(),
    '2026-08-09 12:00:00.123456789'
);

-- a NULL branch must not cancel the conversion for the whole column
INSERT INTO test3 (ts, ts_ns)
SELECT '2026-08-13 12:00:00.001' AS a, '2026-08-13 12:00:00.123456789' AS b
UNION ALL
SELECT '2026-08-14 12:00:00.001', NULL;

-- NULL in the first branch: the union's schema starts out as Null
INSERT INTO test3 (ts, ts_ns)
SELECT '2026-08-15 12:00:00.001' AS a, NULL AS b
UNION ALL
SELECT '2026-08-19 12:00:00.001', '2026-08-19 12:00:00.123456789';

-- the assignment cast also lands on non-literal VALUES expressions
INSERT INTO test3 (ts, st) VALUES (concat('2026-08-20 ', '12:00:00.001'), now());

SELECT ts, ts_ns FROM test3 ORDER BY ts;

-- UNION dedup keys must stay on the source strings: these two spell the same
-- instant differently, so the source query yields two rows and both are kept.
CREATE TABLE test4 (ts TIMESTAMP TIME INDEX) WITH ('append_mode'='true');

INSERT INTO test4 (ts)
SELECT '2026-08-06 04:00:00' UNION SELECT '2026-08-06 04:00:00.000';

SELECT count(*) FROM test4;

SELECT ts FROM test4 ORDER BY ts;

SET time_zone = 'UTC';

DROP TABLE test1;

DROP TABLE test2;

DROP TABLE test3;

DROP TABLE test4;
