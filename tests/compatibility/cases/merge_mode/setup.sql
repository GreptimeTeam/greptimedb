CREATE TABLE merge_mode(
    host STRING,
    ts TIMESTAMP TIME INDEX,
    cpu DOUBLE,
    memory DOUBLE,
    PRIMARY KEY(host)
)
ENGINE=mito
WITH('merge_mode'='last_non_null');

INSERT INTO merge_mode VALUES ('host1', 0, 0, NULL), ('host2', 1, NULL, 1);

INSERT INTO merge_mode VALUES ('host1', 0, NULL, 10), ('host2', 1, 11, NULL);

INSERT INTO merge_mode VALUES ('host1', 0, 20, NULL);

INSERT INTO merge_mode VALUES ('host1', 0, NULL, NULL);

ADMIN FLUSH_TABLE('merge_mode');
