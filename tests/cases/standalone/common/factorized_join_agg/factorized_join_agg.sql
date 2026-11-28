-- Factorized join aggregation behind the experimental opt-in switch.
-- Covers: rewrite off by default, SET enabling it, AVG/SUM/COUNT result parity,
-- SET false overriding, EXPLAIN showing the partial-aggregate shape, the
-- COUNT(*) shape that must stay on the plain aggregate, and partitioned tables,
-- where every branch goes down as `__*_state` with `__*_merge` above the scan.

-- SQLNESS PROTOCOL MYSQL
CREATE TABLE ffx_metrics (
    ts TIMESTAMP TIME INDEX,
    mhost STRING,
    mval DOUBLE
);

-- SQLNESS PROTOCOL MYSQL
CREATE TABLE ffx_nodes (
    ts TIMESTAMP TIME INDEX,
    nhost STRING,
    area STRING,
    wgt DOUBLE
);

-- SQLNESS PROTOCOL MYSQL
INSERT INTO ffx_metrics VALUES
    ('2024-01-01 00:00:00', 'm1', 1.0),
    ('2024-01-01 00:00:01', 'm1', 2.0),
    ('2024-01-01 00:00:02', 'm1', NULL),
    ('2024-01-01 00:00:03', 'm2', 4.0);

-- SQLNESS PROTOCOL MYSQL
INSERT INTO ffx_nodes VALUES
    ('2024-01-01 00:00:00', 'm1', 'west', 2.0),
    ('2024-01-01 00:00:01', 'm1', 'east', NULL),
    ('2024-01-01 00:00:02', 'm2', 'west', 0.5);

-- Default (switch off): plain aggregate over the join.
-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- COUNT on the same flat plan: it counts the pairs whose product is non-NULL.
-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, COUNT(m.mval * n.wgt) AS pairs
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- Enable the experimental rewrite for this session.
-- SQLNESS PROTOCOL MYSQL
SET experimental_enable_factorized_join_agg = true;

-- Rewritten path: identical results (NULL group preserved).
-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- SUM variant also rewrites.
-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, SUM(m.mval * n.wgt) AS total
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- Both branch partial aggregates run inside the region plan: each region sends
-- one state row per group it holds rows for instead of every raw row, and the
-- merge aggregates above the merge scans collapse them.
-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT m.mhost, n.area, SUM(m.mval * n.wgt) AS total
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area;

-- COUNT variant: same answers as the flat plan, a NULL payload counting 0
-- rather than turning the group into NULL.
-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, COUNT(m.mval * n.wgt) AS pairs
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- COUNT rebuilds the pair count from the two partial counts alone: no payload
-- sum appears in its plan.
-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT m.mhost, n.area, COUNT(m.mval * n.wgt) AS pairs
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area;

-- COUNT(*) counts join pairs instead of non-NULL products: a separate case
-- that must keep the plain aggregate.
-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, COUNT(*) AS pair_rows
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT m.mhost, n.area, COUNT(*) AS pair_rows
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area;

-- Grouping by the right-side join key keeps the matching key pair intact.
-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost AS left_host, n.nhost AS right_host, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.nhost ORDER BY m.mhost, n.nhost;

-- The rewrite appears in the plan as partial count/sum aggregates joined.
-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT m.mhost, n.area, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area;

-- A shape that must NOT rewrite: single-column AVG.
-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, AVG(m.mval) AS score
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- A fact table partitioned by a column that is not a group key: neither branch's
-- aggregate is *proven* complete inside one region, so both go down as
-- `__*_state` with `__*_merge` on the frontend. m1's rows are in both partitions,
-- so one of its groups spans two regions.
-- SQLNESS PROTOCOL MYSQL
CREATE TABLE ffx_metrics_dc (
    ts TIMESTAMP TIME INDEX,
    dc STRING,
    mhost STRING,
    mval DOUBLE,
    PRIMARY KEY (dc),
) PARTITION ON COLUMNS (dc) (
    dc < 'p',
    dc >= 'p'
);

-- SQLNESS PROTOCOL MYSQL
INSERT INTO ffx_metrics_dc VALUES
    ('2024-01-01 00:00:00', 'a', 'm1', 1.0),
    ('2024-01-01 00:00:01', 'a', 'm1', NULL),
    ('2024-01-01 00:00:02', 'z', 'm1', 2.0),
    ('2024-01-01 00:00:03', 'z', 'm2', 4.0);

-- One answer row per group, even though m1's rows live in two regions.
-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics_dc m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- The region plans compute the states and the frontend merges them.
-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT m.mhost, n.area, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics_dc m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area;

-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, COUNT(m.mval * n.wgt) AS pairs
FROM ffx_metrics_dc m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT m.mhost, n.area, COUNT(m.mval * n.wgt) AS pairs
FROM ffx_metrics_dc m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area;

-- A fact table partitioned by the join key. The fact branch's group keys cover
-- the table's partition columns, but that is the catalog's key, not the route the
-- query runs against, so the branch still goes down as `__*_state`/`__*_merge`.
-- The key is an integer because a string primary key - which is what a partition
-- column has to be - is dictionary encoded, and a join key carrying a cast is
-- outside the rewrite's shape.
-- SQLNESS PROTOCOL MYSQL
CREATE TABLE ffx_metrics_mid (
    ts TIMESTAMP TIME INDEX,
    mid INT64,
    mhost STRING,
    mval DOUBLE,
    PRIMARY KEY (mid),
) PARTITION ON COLUMNS (mid) (
    mid < 2,
    mid >= 2
);

-- SQLNESS PROTOCOL MYSQL
CREATE TABLE ffx_nodes_mid (
    ts TIMESTAMP TIME INDEX,
    did INT64,
    area STRING,
    wgt DOUBLE
);

-- SQLNESS PROTOCOL MYSQL
INSERT INTO ffx_metrics_mid VALUES
    ('2024-01-01 00:00:00', 1, 'm1', 1.0),
    ('2024-01-01 00:00:01', 1, 'm1', 2.0),
    ('2024-01-01 00:00:02', 1, 'm1', NULL),
    ('2024-01-01 00:00:03', 2, 'm2', 4.0);

-- SQLNESS PROTOCOL MYSQL
INSERT INTO ffx_nodes_mid VALUES
    ('2024-01-01 00:00:00', 1, 'west', 2.0),
    ('2024-01-01 00:00:01', 1, 'east', NULL),
    ('2024-01-01 00:00:02', 2, 'west', 0.5);

-- SQLNESS PROTOCOL MYSQL
SELECT m.mid, n.area, SUM(m.mval * n.wgt) AS total
FROM ffx_metrics_mid m JOIN ffx_nodes_mid n ON m.mid = n.did
GROUP BY m.mid, n.area ORDER BY m.mid, n.area;

-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT m.mid, n.area, SUM(m.mval * n.wgt) AS total
FROM ffx_metrics_mid m JOIN ffx_nodes_mid n ON m.mid = n.did
GROUP BY m.mid, n.area;

-- A DECIMAL payload product is out of range where the flat plan evaluates it,
-- so the count rewrite must decline it instead of answering a group the
-- baseline errors on: the plan keeps the plain aggregate.
-- SQLNESS PROTOCOL MYSQL
CREATE TABLE ffx_metrics_dec (
    ts TIMESTAMP TIME INDEX,
    mhost STRING,
    mval DECIMAL(38, 0)
);

-- SQLNESS PROTOCOL MYSQL
CREATE TABLE ffx_nodes_dec (
    ts TIMESTAMP TIME INDEX,
    nhost STRING,
    area STRING,
    wgt DECIMAL(38, 0)
);

-- SQLNESS PROTOCOL MYSQL
INSERT INTO ffx_metrics_dec VALUES
    ('2024-01-01 00:00:00', 'm1', 1000000000000000000000000000000);

-- SQLNESS PROTOCOL MYSQL
INSERT INTO ffx_nodes_dec VALUES
    ('2024-01-01 00:00:00', 'm1', 'west', 1000000000000000000000000000000);

-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT m.mhost, n.area, COUNT(m.mval * n.wgt) AS pairs
FROM ffx_metrics_dec m JOIN ffx_nodes_dec n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area;

-- SQLNESS PROTOCOL MYSQL
DROP TABLE ffx_metrics_dec;

-- SQLNESS PROTOCOL MYSQL
DROP TABLE ffx_nodes_dec;

-- Session SET false turns the rewrite back off.
-- SQLNESS PROTOCOL MYSQL
SET experimental_enable_factorized_join_agg = false;

-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- SET false restores the plain aggregate plan without partial aliases.
-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT m.mhost, n.area, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area;

-- Answers on the partitioned tables do not move either: the same queries as with
-- the rewrite on, on the same rows.
-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics_dc m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, COUNT(m.mval * n.wgt) AS pairs
FROM ffx_metrics_dc m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- SQLNESS PROTOCOL MYSQL
SELECT m.mid, n.area, SUM(m.mval * n.wgt) AS total
FROM ffx_metrics_mid m JOIN ffx_nodes_mid n ON m.mid = n.did
GROUP BY m.mid, n.area ORDER BY m.mid, n.area;

-- SQLNESS PROTOCOL MYSQL
DROP TABLE ffx_metrics_dc;

-- SQLNESS PROTOCOL MYSQL
DROP TABLE ffx_metrics_mid;

-- SQLNESS PROTOCOL MYSQL
DROP TABLE ffx_nodes_mid;

-- SQLNESS PROTOCOL MYSQL
DROP TABLE ffx_metrics;

-- SQLNESS PROTOCOL MYSQL
DROP TABLE ffx_nodes;
