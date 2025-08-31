-- SQLNESS ARG version=v0.9.5
CREATE TABLE alter_test (
    id INT,
    val STRING,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(id)
);

INSERT INTO alter_test(id, val, ts) VALUES (1, 'a', 1672531200000);

-- SQLNESS ARG version=latest
ALTER TABLE alter_test ADD COLUMN new_col INT;

SHOW CREATE TABLE alter_test;

INSERT INTO alter_test(id, val, ts, new_col) VALUES (2, 'b', 1672531201000, 100);

SELECT * FROM alter_test ORDER BY id;

DROP TABLE alter_test;
