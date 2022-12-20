CREATE TABLE integers(i BIGINT TIME INDEX);

INSERT INTO integers VALUES (0), (1), (2), (3), (4);

SELECT * FROM integers UNION ALL SELECT * FROM integers LIMIT 7;

SELECT COUNT(*) FROM (SELECT * FROM integers UNION ALL SELECT * FROM integers LIMIT 7) tbl;

DROP TABLE integers;
