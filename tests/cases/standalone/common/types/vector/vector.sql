CREATE TABLE t (ts TIMESTAMP TIME INDEX, v VECTOR(3));

INSERT INTO t VALUES
(1, "[1.0, 2.0, 3.0]"),
(2, "[4.0, 5.0, 6.0]"),
(3, "[7.0, 8.0, 9.0]");

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM t;

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM t;

SELECT round(vec_cos_distance(v, '[0.0, 0.0, 0.0]'), 2) FROM t;

SELECT *, round(vec_cos_distance(v, '[0.0, 0.0, 0.0]'), 2) as d FROM t ORDER BY d;

SELECT round(vec_cos_distance('[7.0, 8.0, 9.0]', v), 2) FROM t;

SELECT *, round(vec_cos_distance('[7.0, 8.0, 9.0]', v), 2) as d FROM t ORDER BY d;

SELECT round(vec_cos_distance(v, v), 2) FROM t;

-- Unexpected dimension --
SELECT vec_cos_distance(v, '[1.0]') FROM t;

-- Invalid type --
SELECT vec_cos_distance(v, 1.0) FROM t;

SELECT round(vec_l2sq_distance(v, '[0.0, 0.0, 0.0]'), 2) FROM t;

SELECT *, round(vec_l2sq_distance(v, '[0.0, 0.0, 0.0]'), 2) as d FROM t ORDER BY d;

SELECT round(vec_l2sq_distance('[7.0, 8.0, 9.0]', v), 2) FROM t;

SELECT *, round(vec_l2sq_distance('[7.0, 8.0, 9.0]', v), 2) as d FROM t ORDER BY d;

SELECT round(vec_l2sq_distance(v, v), 2) FROM t;

-- Unexpected dimension --
SELECT vec_l2sq_distance(v, '[1.0]') FROM t;

-- Invalid type --
SELECT vec_l2sq_distance(v, 1.0) FROM t;


SELECT round(vec_dot_product(v, '[0.0, 0.0, 0.0]'), 2) FROM t;

SELECT *, round(vec_dot_product(v, '[0.0, 0.0, 0.0]'), 2) as d FROM t ORDER BY d;

SELECT round(vec_dot_product('[7.0, 8.0, 9.0]', v), 2) FROM t;

SELECT *, round(vec_dot_product('[7.0, 8.0, 9.0]', v), 2) as d FROM t ORDER BY d;

SELECT round(vec_dot_product(v, v), 2) FROM t;

-- Unexpected dimension --
SELECT vec_dot_product(v, '[1.0]') FROM t;

-- Invalid type --
SELECT vec_dot_product(v, 1.0) FROM t;

-- Unexpected dimension --
INSERT INTO t VALUES
(4, "[1.0]");

-- Invalid vector value --
INSERT INTO t VALUES
(5, "1.0,2.0,3.0");

-- Invalid vector value --
INSERT INTO t VALUES
(6, "[30h, 40s, 50m]");

CREATE TABLE t2 (ts TIMESTAMP TIME INDEX, v VECTOR(3) DEFAULT '[1.0, 2.0, 3.0]');

INSERT INTO t2 (ts) VALUES
(1),
(2),
(3);

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM t2;

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM t2;

DROP TABLE t;

DROP TABLE t2;
