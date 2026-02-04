-- Vector index join/subquery coverage

-- ============================================
-- Part 1: Join should not pre-limit left table
-- ============================================
CREATE TABLE vectors_join_left (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(2) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

CREATE TABLE vectors_join_right (
    vec_id INT,
    note STRING,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (vec_id)
);

INSERT INTO vectors_join_left VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.9, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.0, 1.0]'),
    (4, '2024-01-01 00:00:03', '[0.0, 0.9]');

INSERT INTO vectors_join_right VALUES
    (3, 'keep', '2024-01-01 00:00:02'),
    (4, 'keep', '2024-01-01 00:00:03');

SELECT l.vec_id, round(vec_l2sq_distance(l.embedding, '[1.0, 0.0]'), 2) AS dist
FROM vectors_join_left l
JOIN vectors_join_right r ON l.vec_id = r.vec_id
ORDER BY dist, l.vec_id
LIMIT 1;

DROP TABLE vectors_join_left;
DROP TABLE vectors_join_right;

-- ============================================
-- Part 2: Subquery should be a barrier
-- ============================================
CREATE TABLE vectors_subquery (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(2) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

INSERT INTO vectors_subquery VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.9, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.0, 1.0]'),
    (4, '2024-01-01 00:00:03', '[0.0, 0.9]');

SELECT s.vec_id, round(vec_l2sq_distance(s.embedding, '[1.0, 0.0]'), 2) AS dist
FROM (
    SELECT * FROM vectors_subquery WHERE vec_id >= 3
) s
ORDER BY dist, s.vec_id
LIMIT 1;

DROP TABLE vectors_subquery;

-- ============================================
-- Part 3: LEFT JOIN should not pre-limit
-- ============================================
CREATE TABLE vectors_left_join (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(2) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

CREATE TABLE vectors_left_join_filter (
    vec_id INT,
    keep BOOLEAN,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (vec_id)
);

INSERT INTO vectors_left_join VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.9, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.0, 1.0]'),
    (4, '2024-01-01 00:00:03', '[0.0, 0.9]');

-- Only vec_id 3,4 have matching rows in filter table
INSERT INTO vectors_left_join_filter VALUES
    (3, true, '2024-01-01 00:00:02'),
    (4, true, '2024-01-01 00:00:03');

-- LEFT JOIN then filter by IS NOT NULL
-- Should return vec_id=4 (dist=1.81), not vec_id=1 or 2
SELECT l.vec_id, round(vec_l2sq_distance(l.embedding, '[1.0, 0.0]'), 2) AS dist
FROM vectors_left_join l
LEFT JOIN vectors_left_join_filter r ON l.vec_id = r.vec_id
WHERE r.vec_id IS NOT NULL
ORDER BY dist, l.vec_id
LIMIT 1;

DROP TABLE vectors_left_join;
DROP TABLE vectors_left_join_filter;

-- ============================================
-- Part 4: Inlineable subquery should allow hint
-- ============================================
CREATE TABLE vectors_inline_subquery (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(2) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

INSERT INTO vectors_inline_subquery VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.9, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.0, 1.0]'),
    (4, '2024-01-01 00:00:03', '[0.0, 0.9]');

ADMIN FLUSH_TABLE('vectors_inline_subquery');

-- Subquery without LIMIT/DISTINCT/aggregation can be inlined
-- Vector hint should be able to push down
SELECT s.vec_id, round(vec_l2sq_distance(s.embedding, '[1.0, 0.0]'), 2) AS dist
FROM (
    SELECT * FROM vectors_inline_subquery WHERE vec_id >= 1
) s
ORDER BY dist, s.vec_id
LIMIT 2;

DROP TABLE vectors_inline_subquery;

-- ============================================
-- Part 5: CTE should be a barrier
-- ============================================
CREATE TABLE vectors_cte (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(2) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

INSERT INTO vectors_cte VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.9, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.0, 1.0]'),
    (4, '2024-01-01 00:00:03', '[0.0, 0.9]');

-- CTE acts as optimization barrier
-- Filter in CTE limits to vec_id >= 3, so result should be vec_id=4 (dist=1.81)
WITH filtered AS (
    SELECT * FROM vectors_cte WHERE vec_id >= 3
)
SELECT vec_id, round(vec_l2sq_distance(embedding, '[1.0, 0.0]'), 2) AS dist
FROM filtered
ORDER BY dist, vec_id
LIMIT 1;

DROP TABLE vectors_cte;
