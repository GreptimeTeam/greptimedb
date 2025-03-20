CREATE TABLE integers(i INTEGER, j TIMESTAMP TIME INDEX);

INSERT INTO integers VALUES (1, 1), (2, 2), (3, 3), (NULL, 4);

SELECT i1.i, i2.i FROM integers i1, integers i2 WHERE i1.i=i2.i ORDER BY 1;

SELECT i1.i,i2.i FROM integers i1, integers i2 WHERE i1.i=i2.i AND i1.i>1 ORDER BY 1;

SELECT i1.i,i2.i,i3.i FROM integers i1, integers i2, integers i3 WHERE i1.i=i2.i AND i1.i=i3.i AND i1.i>1 ORDER BY 1;

SELECT i1.i,i2.i FROM integers i1 JOIN integers i2 ON i1.i=i2.i WHERE i1.i>1 ORDER BY 1;

SELECT i1.i,i2.i FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=1 WHERE i1.i>2 ORDER BY 2;

-- ignore: ON 1=0 is not supported
-- SELECT i1.i,i2.i FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=0 WHERE i2.i IS NOT NULL ORDER BY 2;

-- ignore: IGNORE ON 1=0 is not supported
-- SELECT i1.i,i2.i FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=0 WHERE i2.i>1 ORDER BY 2;

-- ignore: IGNORE ON 1=0 is not supported
-- SELECT i1.i,i2.i FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=0 WHERE CASE WHEN i2.i IS NULL THEN False ELSE True END ORDER BY 2;

SELECT DISTINCT i1.i,i2.i FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=0 WHERE i2.i IS NULL ORDER BY 1;

SELECT i1.i,i2.i FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=1 WHERE i1.i=i2.i ORDER BY 1;

SELECT * FROM integers WHERE i IN ((SELECT i FROM integers)) ORDER BY i;

SELECT * FROM integers WHERE i NOT IN ((SELECT i FROM integers WHERE i=1)) ORDER BY i;

SELECT * FROM integers WHERE i IN ((SELECT i FROM integers)) AND i<3 ORDER BY i;

SELECT i1.i,i2.i FROM integers i1, integers i2 WHERE i IN ((SELECT i FROM integers)) AND i1.i=i2.i ORDER BY 1;

SELECT * FROM integers i1 WHERE EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i1.i;

SELECT * FROM integers i1 WHERE NOT EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i1.i;

SELECT i1.i,i2.i FROM integers i1, integers i2 WHERE i1.i=(SELECT i FROM integers WHERE i1.i=i) AND i1.i=i2.i ORDER BY i1.i;

SELECT * FROM (SELECT i1.i AS a, i2.i AS b FROM integers i1, integers i2) a1 WHERE a=b ORDER BY 1;

SELECT * FROM (SELECT i1.i=i2.i AS cond FROM integers i1, integers i2) a1 WHERE cond ORDER BY 1;

SELECT * FROM (SELECT DISTINCT i1.i AS a, i2.i AS b FROM integers i1, integers i2) res WHERE a=1 AND b=3;

SELECT i FROM (SELECT * FROM integers i1 UNION SELECT * FROM integers i2) a WHERE i=3;

SELECT * FROM (SELECT i1.i AS a, i2.i AS b, row_number() OVER (ORDER BY i1.i, i2.i) FROM integers i1, integers i2 WHERE i1.i IS NOT NULL AND i2.i IS NOT NULL) a1 WHERE a=b ORDER BY 1;

-- TODO(ruihang): Invalid argument error: must either specify a row count or at least one column
-- SELECT * FROM (SELECT 0=1 AS cond FROM integers i1, integers i2) a1 WHERE cond ORDER BY 1;

-- This should be a bug in DataFusion, it use UserDefinedLogicalNode::prevent_predicate_push_down_columns()
-- to prevent pushdown, but this filter is Literal(Boolean(false)). It doesn't reference any column.
SELECT * FROM (SELECT 0=1 AS cond FROM integers i1, integers i2 GROUP BY 1) a1 WHERE cond ORDER BY 1;

DROP TABLE integers;

CREATE TABLE characters(c STRING, t TIMESTAMP TIME INDEX);

INSERT INTO characters VALUES ('a', 1), ('b', 2), ('c', 3), (NULL, 4), ('a', 5), ('b', 6), ('c', 7), (NULL, 8);

SELECT * FROM characters WHERE c IN ('a', 'c') ORDER BY t;

DROP TABLE characters;
