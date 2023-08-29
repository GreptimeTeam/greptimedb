CREATE TABLE integers(i INTEGER, j TIMESTAMP TIME INDEX);

INSERT INTO integers VALUES (1, 1), (2, 2), (3, 3), (NULL, 4);

SELECT i1.i, i2.i FROM integers i1, integers i2 WHERE i1.i=i2.i ORDER BY 1;

SELECT i1.i,i2.i FROM integers i1, integers i2 WHERE i1.i=i2.i AND i1.i>1 ORDER BY 1;

SELECT i1.i,i2.i,i3.i FROM integers i1, integers i2, integers i3 WHERE i1.i=i2.i AND i1.i=i3.i AND i1.i>1 ORDER BY 1;

SELECT i1.i,i2.i FROM integers i1 JOIN integers i2 ON i1.i=i2.i WHERE i1.i>1 ORDER BY 1;

SELECT i1.i,i2.i FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=1 WHERE i1.i>2 ORDER BY 2;

SELECT i1.i,i2.i FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=0 WHERE i2.i IS NOT NULL ORDER BY 2;

SELECT i1.i,i2.i FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=0 WHERE i2.i>1 ORDER BY 2;

SELECT i1.i,i2.i FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=0 WHERE CASE WHEN i2.i IS NULL THEN False ELSE True END ORDER BY 2;

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

-- TODO(LFC): Somehow the following SQL does not order by column 1 under new DataFusion occasionally. Should further investigate it. Comment it out temporarily.
-- expected:
--  +---+---+--------------+
--  | a | b | ROW_NUMBER() |
--  +---+---+--------------+
--  | 1 | 1 | 1            |
--  | 2 | 2 | 5            |
--  | 3 | 3 | 9            |
--  +---+---+--------------+
-- SELECT * FROM (SELECT i1.i AS a, i2.i AS b, row_number() OVER (ORDER BY i1.i, i2.i) FROM integers i1, integers i2 WHERE i1.i IS NOT NULL AND i2.i IS NOT NULL) a1 WHERE a=b ORDER BY 1;

SELECT * FROM (SELECT 0=1 AS cond FROM integers i1, integers i2) a1 WHERE cond ORDER BY 1;

SELECT * FROM (SELECT 0=1 AS cond FROM integers i1, integers i2 GROUP BY 1) a1 WHERE cond ORDER BY 1;

DROP TABLE integers;
