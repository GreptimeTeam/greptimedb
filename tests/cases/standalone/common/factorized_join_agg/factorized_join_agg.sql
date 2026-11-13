-- Factorized join aggregation behind the experimental opt-in switch.
-- Covers: rewrite off by default, SET enabling it, result parity, SET false
-- overriding, and EXPLAIN showing the partial-aggregate shape.

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

-- Grouping by the right-side join key keeps the matching key pair intact.
-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost AS left_host, n.nhost AS right_host, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.nhost ORDER BY m.mhost, n.nhost;

-- The rewrite appears in the plan as partial count/sum aggregates joined.
-- SQLNESS PROTOCOL MYSQL
EXPLAIN SELECT m.mhost, n.area, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area;

-- A shape that must NOT rewrite: single-column AVG.
-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, AVG(m.mval) AS score
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- Session SET false turns the rewrite back off.
-- SQLNESS PROTOCOL MYSQL
SET experimental_enable_factorized_join_agg = false;

-- SQLNESS PROTOCOL MYSQL
SELECT m.mhost, n.area, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area ORDER BY m.mhost, n.area;

-- SET false restores the plain aggregate plan without partial aliases.
-- SQLNESS PROTOCOL MYSQL
EXPLAIN SELECT m.mhost, n.area, AVG(m.mval * n.wgt) AS score
FROM ffx_metrics m JOIN ffx_nodes n ON m.mhost = n.nhost
GROUP BY m.mhost, n.area;

-- SQLNESS PROTOCOL MYSQL
DROP TABLE ffx_metrics;

-- SQLNESS PROTOCOL MYSQL
DROP TABLE ffx_nodes;
