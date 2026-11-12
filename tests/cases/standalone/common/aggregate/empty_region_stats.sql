-- Append-mode regions report exact row counts, so the datanode can answer
-- count/min/max over a region from statistics. This only happens when every
-- aggregate in the query can be answered that way.
CREATE TABLE a1 (ts TIMESTAMP(9) TIME INDEX, k STRING, v BIGINT)
PARTITION ON COLUMNS (k) (k < 'b', k >= 'b' AND k < 'c', k >= 'c')
WITH (append_mode = 'true');

-- All regions are empty.
SELECT min(v), max(v) FROM a1;

SELECT count(*), count(v) FROM a1;

INSERT INTO a1 VALUES (1000, 'a', 1), (2000, 'a', 2);

-- Two of three regions are empty.
SELECT min(v) FROM a1;

SELECT max(v) FROM a1;

SELECT min(ts), max(k) FROM a1;

SELECT count(*), count(v) FROM a1;

SELECT min(v), count(*) FROM a1;

SELECT sum(v), avg(v) FROM a1;

SELECT k, min(v), max(v), count(*) FROM a1 GROUP BY k ORDER BY k;

INSERT INTO a1 VALUES (3000, 'b', 3);

-- One of three regions is empty.
SELECT min(v), max(v) FROM a1;

SELECT k, min(v), max(v), count(*) FROM a1 GROUP BY k ORDER BY k;

INSERT INTO a1 VALUES (4000, 'c', 4);

-- No region is empty.
SELECT min(v), max(v), count(*) FROM a1;

DROP TABLE a1;
