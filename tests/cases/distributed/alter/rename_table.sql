CREATE TABLE t(i INTEGER, j BIGINT TIME INDEX);

DESC TABLE t;

INSERT INTO TABLE t VALUES (1, 1), (3, 3), (NULL, 4);

SELECT * from t;

-- TODO(LFC): Port test cases from standalone env when distribute rename table is implemented (#723).
ALTER TABLE t RENAME new_table;

DESC TABLE t;

DROP TABLE t;
