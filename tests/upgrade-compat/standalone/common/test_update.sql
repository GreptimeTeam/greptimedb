-- SQLNESS ARG version=v0.9.5
CREATE TABLE update_test (
    id INT,
    val INT,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(id)
);

INSERT INTO update_test(id, val, ts) VALUES (1, 10, 1672531200000), (2, 20, 1672531201000);

-- SQLNESS ARG version=latest
UPDATE update_test SET val = 100 WHERE id = 1;

SELECT * FROM update_test ORDER BY id;

DROP TABLE update_test;
