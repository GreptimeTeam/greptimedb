-- SQLNESS ARG version=v0.9.5
CREATE TABLE alter_drop_test (
    id INT,
    val_to_drop STRING,
    val_to_keep STRING,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(id)
);

INSERT INTO alter_drop_test(id, val_to_drop, val_to_keep, ts) VALUES (1, 'a', 'x', 1672531200000);

-- SQLNESS ARG version=latest
ALTER TABLE alter_drop_test DROP COLUMN val_to_drop;

SHOW CREATE TABLE alter_drop_test;

SELECT * FROM alter_drop_test ORDER BY id;

INSERT INTO alter_drop_test(id, val_to_keep, ts) VALUES (2, 'y', 1672531201000);

SELECT * FROM alter_drop_test ORDER BY id;

DROP TABLE alter_drop_test;
