-- Test vector index creation and KNN search

-- ============================================
-- Part 1: Basic L2 squared distance tests
-- ============================================

-- Create a table with vector column and L2sq vector index
CREATE TABLE vectors_l2sq (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(4) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

-- Insert test vectors
INSERT INTO vectors_l2sq VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0, 0.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.0, 1.0, 0.0, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.0, 0.0, 1.0, 0.0]'),
    (4, '2024-01-01 00:00:03', '[0.0, 0.0, 0.0, 1.0]'),
    (5, '2024-01-01 00:00:04', '[1.0, 1.0, 0.0, 0.0]'),
    (6, '2024-01-01 00:00:05', '[0.0, 1.0, 1.0, 0.0]'),
    (7, '2024-01-01 00:00:06', '[0.0, 0.0, 1.0, 1.0]'),
    (8, '2024-01-01 00:00:07', '[1.0, 0.0, 0.0, 1.0]');

-- Query BEFORE flush (memtable search)
-- Expected: vec_id=1 (distance=0), vec_id=5 (distance=1), vec_id=8 (distance=1)
SELECT vec_id, vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') as distance
FROM vectors_l2sq
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 3;

-- Flush to create SST files with vector index
ADMIN FLUSH_TABLE('vectors_l2sq');

-- Query AFTER flush (SST index search)
SELECT vec_id, vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') as distance
FROM vectors_l2sq
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 3;

-- Query with different target vector
-- Expected: vec_id=6 (distance=0), vec_id=2 (distance=1), vec_id=3 (distance=1)
SELECT vec_id, vec_l2sq_distance(embedding, '[0.0, 1.0, 1.0, 0.0]') as distance
FROM vectors_l2sq
ORDER BY vec_l2sq_distance(embedding, '[0.0, 1.0, 1.0, 0.0]'), vec_id
LIMIT 3;

DROP TABLE vectors_l2sq;

-- ============================================
-- Part 2: Cosine distance tests
-- ============================================

CREATE TABLE vectors_cosine (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(4) NOT NULL VECTOR INDEX WITH (metric = 'cosine'),
    PRIMARY KEY (vec_id)
);

-- Insert vectors with different magnitudes but same/different directions
INSERT INTO vectors_cosine VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0, 0.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[2.0, 0.0, 0.0, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.0, 1.0, 0.0, 0.0]'),
    (4, '2024-01-01 00:00:03', '[1.0, 1.0, 0.0, 0.0]'),
    (5, '2024-01-01 00:00:04', '[-1.0, 0.0, 0.0, 0.0]');

-- Memtable search with cosine distance
-- vec_id=1 and vec_id=2 should have same cosine distance (0) since they point same direction
SELECT vec_id, vec_cos_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') as distance
FROM vectors_cosine
ORDER BY vec_cos_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 3;

ADMIN FLUSH_TABLE('vectors_cosine');

-- SST index search with cosine distance
SELECT vec_id, vec_cos_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') as distance
FROM vectors_cosine
ORDER BY vec_cos_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 3;

DROP TABLE vectors_cosine;

-- ============================================
-- Part 3: Dot product (inner product) tests
-- ============================================

CREATE TABLE vectors_dot (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(4) NOT NULL VECTOR INDEX WITH (metric = 'dot'),
    PRIMARY KEY (vec_id)
);

INSERT INTO vectors_dot VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0, 0.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[2.0, 0.0, 0.0, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.0, 1.0, 0.0, 0.0]'),
    (4, '2024-01-01 00:00:03', '[1.0, 1.0, 0.0, 0.0]'),
    (5, '2024-01-01 00:00:04', '[-1.0, 0.0, 0.0, 0.0]');

-- Memtable search with dot product
-- Larger dot product means more similar, so we use negative for ordering
-- vec_id=2 should be best (dot=2), vec_id=1 and vec_id=4 have dot=1
SELECT vec_id, vec_dot_product(embedding, '[1.0, 0.0, 0.0, 0.0]') as dot_product
FROM vectors_dot
ORDER BY vec_dot_product(embedding, '[1.0, 0.0, 0.0, 0.0]') DESC, vec_id
LIMIT 3;

ADMIN FLUSH_TABLE('vectors_dot');

-- SST index search with dot product
SELECT vec_id, vec_dot_product(embedding, '[1.0, 0.0, 0.0, 0.0]') as dot_product
FROM vectors_dot
ORDER BY vec_dot_product(embedding, '[1.0, 0.0, 0.0, 0.0]') DESC, vec_id
LIMIT 3;

DROP TABLE vectors_dot;

-- ============================================
-- Part 4: NULL vector handling tests
-- ============================================

CREATE TABLE vectors_null (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(4) VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

-- Insert vectors with some NULLs
INSERT INTO vectors_null VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0, 0.0, 0.0]'),
    (2, '2024-01-01 00:00:01', NULL),
    (3, '2024-01-01 00:00:02', '[0.0, 1.0, 0.0, 0.0]'),
    (4, '2024-01-01 00:00:03', NULL),
    (5, '2024-01-01 00:00:04', '[0.0, 0.0, 1.0, 0.0]');

-- Memtable search should skip NULL vectors
SELECT vec_id, vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') as distance
FROM vectors_null
WHERE embedding IS NOT NULL
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 3;

ADMIN FLUSH_TABLE('vectors_null');

-- SST index search should also skip NULL vectors
SELECT vec_id, vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') as distance
FROM vectors_null
WHERE embedding IS NOT NULL
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 3;

DROP TABLE vectors_null;

-- ============================================
-- Part 5: Mixed memtable + SST search tests
-- ============================================

CREATE TABLE vectors_mixed (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(4) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

-- Insert first batch and flush to SST
INSERT INTO vectors_mixed VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0, 0.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.0, 1.0, 0.0, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.0, 0.0, 1.0, 0.0]');

ADMIN FLUSH_TABLE('vectors_mixed');

-- Insert second batch (stays in memtable)
INSERT INTO vectors_mixed VALUES
    (4, '2024-01-01 00:00:03', '[0.5, 0.5, 0.0, 0.0]'),
    (5, '2024-01-01 00:00:04', '[0.9, 0.1, 0.0, 0.0]');

-- Query should search both SST (vec_id 1,2,3) and memtable (vec_id 4,5)
-- Target: [1.0, 0.0, 0.0, 0.0]
-- Expected: vec_id=1 (dist=0), vec_id=5 (dist=0.02), vec_id=4 (dist=0.5)
SELECT vec_id, vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') as distance
FROM vectors_mixed
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 3;

DROP TABLE vectors_mixed;

-- ============================================
-- Part 6: KNN with WHERE clause tests
-- ============================================

CREATE TABLE vectors_filter (
    vec_id INT,
    "category" STRING,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(4) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

INSERT INTO vectors_filter VALUES
    (1, 'A', '2024-01-01 00:00:00', '[1.0, 0.0, 0.0, 0.0]'),
    (2, 'B', '2024-01-01 00:00:01', '[0.9, 0.1, 0.0, 0.0]'),
    (3, 'A', '2024-01-01 00:00:02', '[0.0, 1.0, 0.0, 0.0]'),
    (4, 'B', '2024-01-01 00:00:03', '[0.1, 0.9, 0.0, 0.0]'),
    (5, 'A', '2024-01-01 00:00:04', '[0.5, 0.5, 0.0, 0.0]');

-- Memtable search with filter
SELECT vec_id, category, vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') as distance
FROM vectors_filter
WHERE category = 'A'
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 2;

ADMIN FLUSH_TABLE('vectors_filter');

-- SST index search with filter
SELECT vec_id, category, vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') as distance
FROM vectors_filter
WHERE category = 'A'
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 2;

-- Filter with time range
SELECT vec_id, vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') as distance
FROM vectors_filter
WHERE ts >= '2024-01-01 00:00:02' AND ts <= '2024-01-01 00:00:04'
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 2;

DROP TABLE vectors_filter;

-- ============================================
-- Part 7: Higher dimension vectors
-- ============================================

CREATE TABLE vectors_high_dim (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(128) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

-- Insert high-dimensional vectors (simplified: first few elements differ)
INSERT INTO vectors_high_dim VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]');

-- Memtable search
SELECT vec_id FROM vectors_high_dim
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 2;

ADMIN FLUSH_TABLE('vectors_high_dim');

-- SST index search
SELECT vec_id FROM vectors_high_dim
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 2;

DROP TABLE vectors_high_dim;

-- ============================================
-- Part 8: Different k values (LIMIT)
-- ============================================

CREATE TABLE vectors_k (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(4) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

INSERT INTO vectors_k VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0, 0.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.9, 0.1, 0.0, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.8, 0.2, 0.0, 0.0]'),
    (4, '2024-01-01 00:00:03', '[0.7, 0.3, 0.0, 0.0]'),
    (5, '2024-01-01 00:00:04', '[0.6, 0.4, 0.0, 0.0]'),
    (6, '2024-01-01 00:00:05', '[0.5, 0.5, 0.0, 0.0]'),
    (7, '2024-01-01 00:00:06', '[0.4, 0.6, 0.0, 0.0]'),
    (8, '2024-01-01 00:00:07', '[0.3, 0.7, 0.0, 0.0]'),
    (9, '2024-01-01 00:00:08', '[0.2, 0.8, 0.0, 0.0]'),
    (10, '2024-01-01 00:00:09', '[0.1, 0.9, 0.0, 0.0]');

ADMIN FLUSH_TABLE('vectors_k');

-- k=1
SELECT vec_id FROM vectors_k
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 1;

-- k=5
SELECT vec_id FROM vectors_k
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 5;

-- k=10 (all vectors)
SELECT vec_id FROM vectors_k
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 10;

DROP TABLE vectors_k;

-- ============================================
-- Part 9: Engine parameter tests
-- ============================================

-- Create table with explicit engine parameter (usearch is default)
CREATE TABLE vectors_engine (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(4) NOT NULL VECTOR INDEX WITH (engine = 'usearch', metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

-- Insert test vectors
INSERT INTO vectors_engine VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0, 0.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.0, 1.0, 0.0, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.5, 0.5, 0.0, 0.0]');

-- Memtable search
SELECT vec_id FROM vectors_engine
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 2;

ADMIN FLUSH_TABLE('vectors_engine');

-- SST index search with usearch engine
SELECT vec_id FROM vectors_engine
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0, 0.0, 0.0]'), vec_id
LIMIT 2;

DROP TABLE vectors_engine;
