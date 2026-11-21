-- The distributed nested broadcast join rewrite of the query engine.
--
-- `SET experimental_dist_join = true` opts the session into the rewrite: it nests the build
-- side's MergeScan inside the probe side's MergeScan, so that the join runs on the datanodes
-- that own the probe regions. The rewrite is additionally gated by the region statistics that
-- the datanodes report in their heartbeats: every region of both candidate tables has to have
-- a non-zero reported size, and broadcasting the build table to the probe regions has to be
-- cheaper than reading the probe table itself.
--
-- The tables below are a partitioned fact table (the probe side, three regions) that is much
-- larger than the small build table (two regions), so that the cost heuristic favors the
-- rewrite as soon as the statistics are available. The join keys are duplicated on both sides,
-- a NULL key on both sides matches nothing, one key of each side has no partner, the residual
-- predicate rejects the pairs whose values sum to at most 105, and the probe predicate below
-- both prunes the third probe region and excludes a probe row that would otherwise match.
--
-- `dist_join_gen` is a tiny numbers table, only used to pad the probe table below.

CREATE TABLE dist_join_gen (n INT, ts TIMESTAMP TIME INDEX);

INSERT INTO dist_join_gen VALUES
    (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9);

CREATE TABLE dist_join_probe (
    p_id INT,
    p_key INT,
    p_val INT,
    p_pad STRING,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (p_id)
) PARTITION ON COLUMNS (p_id) (
    p_id < 100,
    p_id >= 100 AND p_id < 200,
    p_id >= 200
);

CREATE TABLE dist_join_build (
    b_id INT,
    b_key INT,
    b_val INT,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (b_id)
) PARTITION ON COLUMNS (b_id) (
    b_id < 100,
    b_id >= 100
);

-- The rows of the join: the probe rows of key 10 join both rows of the duplicated build key
-- (601, 602 and 701, 702), the probe row of key 10 with value 100 is rejected by the residual
-- predicate (101, 102), key 20 joins across the two build partitions, key 30 has no build
-- partner, key 99 has no probe partner, the NULL keys match nothing, and the probe row 301 in
-- the third region matches too but is excluded by the probe predicate of the query below.
INSERT INTO dist_join_probe (p_id, p_key, p_val, p_pad, ts) VALUES
    (1, 10, 100, '', 1001),
    (2, 10, 600, '', 1002),
    (3, NULL, 700, '', 1003),
    (4, 20, 200, '', 1004),
    (5, 30, 30, '', 1005),
    (6, 10, 700, '', 1006),
    (301, 10, 800, '', 1007);

INSERT INTO dist_join_build (b_id, b_key, b_val, ts) VALUES
    (1, 10, 1, 2001),
    (2, 10, 2, 2002),
    (101, 20, 3, 2003),
    (102, 99, 4, 2004),
    (103, NULL, 5, 2005);

-- Padding of the third probe region: it makes the probe table larger than the whole build
-- table, the comparison of the cost heuristic. These rows join with no build row.
INSERT INTO dist_join_probe (p_id, p_key, p_val, p_pad, ts)
SELECT n1.n + n2.n * 10 + n3.n * 100 + 1000,
       -1,
       0,
       'padding-padding-padding-padding-padding',
       1
FROM dist_join_gen n1, dist_join_gen n2, dist_join_gen n3;

-- The region statistics are reported in the datanodes' heartbeats (every second in this
-- environment, and the metasrv flushes them on every heartbeat), so a heartbeat has to pass
-- after the inserts before the rewrite can see the sizes of the regions above.
-- SQLNESS PROTOCOL MYSQL
-- SQLNESS SLEEP 3s
SET experimental_dist_join = false;

-- Without the session opt-in the join keeps its plain distributed plan: both sides are read
-- with their own MergeScan and the join runs on the frontend.
-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT p.p_id, p.p_key, p.p_val, b.b_id, b.b_key, b.b_val
FROM dist_join_probe p INNER JOIN dist_join_build b
ON p.p_key = b.b_key AND p.p_val + b.b_val > 105
WHERE p.p_id < 200
ORDER BY p.p_val, p.p_id, b.b_val, b.b_id;

-- With the opt-in the build side is nested into the probe side's MergeScan.
-- SQLNESS PROTOCOL MYSQL
SET experimental_dist_join = true;

-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT p.p_id, p.p_key, p.p_val, b.b_id, b.b_key, b.b_val
FROM dist_join_probe p INNER JOIN dist_join_build b
ON p.p_key = b.b_key AND p.p_val + b.b_val > 105
WHERE p.p_id < 200
ORDER BY p.p_val, p.p_id, b.b_val, b.b_id;

-- The rewritten plan returns the same rows as the plain one.
-- SQLNESS PROTOCOL MYSQL
SELECT p.p_id, p.p_key, p.p_val, b.b_id, b.b_key, b.b_val
FROM dist_join_probe p INNER JOIN dist_join_build b
ON p.p_key = b.b_key AND p.p_val + b.b_val > 105
WHERE p.p_id < 200
ORDER BY p.p_val, p.p_id, b.b_val, b.b_id;

-- Unsetting the opt-in reverts the plan.
-- SQLNESS PROTOCOL MYSQL
SET experimental_dist_join = false;

-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT p.p_id, p.p_key, p.p_val, b.b_id, b.b_key, b.b_val
FROM dist_join_probe p INNER JOIN dist_join_build b
ON p.p_key = b.b_key AND p.p_val + b.b_val > 105
WHERE p.p_id < 200
ORDER BY p.p_val, p.p_id, b.b_val, b.b_id;

DROP TABLE dist_join_probe;

DROP TABLE dist_join_build;

DROP TABLE dist_join_gen;
