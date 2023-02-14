CREATE TABLE integers(i INTEGER, j BIGINT TIME INDEX);

INSERT INTO integers VALUES (1, 1), (2, 2), (3, 3), (NULL, 4);

SELECT i1.i, i2.i FROM integers i1, integers i2 WHERE i1.i=i2.i ORDER BY 1;

SELECT i1.i,i2.i FROM integers i1, integers i2 WHERE i1.i=i2.i AND i1.i>1 ORDER BY 1;

SELECT i1.i,i2.i,i3.i FROM integers i1, integers i2, integers i3 WHERE i1.i=i2.i AND i1.i=i3.i AND i1.i>1 ORDER BY 1;

SELECT i1.i,i2.i FROM integers i1 JOIN integers i2 ON i1.i=i2.i WHERE i1.i>1 ORDER BY 1;

-- TODO(LFC): Resolve #790, then port remaining test case from standalone.

DROP TABLE integers;
