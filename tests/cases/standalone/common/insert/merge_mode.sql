create table if not exists last_non_null_table(
    host string,
    ts timestamp,
    cpu double,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('merge_mode'='last_non_null');

INSERT INTO last_non_null_table VALUES ('host1', 0, 0, NULL), ('host2', 1, NULL, 1);

INSERT INTO last_non_null_table VALUES ('host1', 0, NULL, 10), ('host2', 1, 11, NULL);

SELECT * from last_non_null_table ORDER BY host, ts;

INSERT INTO last_non_null_table VALUES ('host1', 0, 20, NULL);

SELECT * from last_non_null_table ORDER BY host, ts;

INSERT INTO last_non_null_table VALUES ('host1', 0, NULL, NULL);

SELECT * from last_non_null_table ORDER BY host, ts;

DROP TABLE last_non_null_table;

create table if not exists last_row_table(
    host string,
    ts timestamp,
    cpu double,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('merge_mode'='last_row');

INSERT INTO last_row_table VALUES ('host1', 0, 0, NULL), ('host2', 1, NULL, 1);

INSERT INTO last_row_table VALUES ('host1', 0, NULL, 10), ('host2', 1, 11, NULL);

SELECT * from last_row_table ORDER BY host, ts;

DROP TABLE last_row_table;

create table if not exists invalid_merge_mode(
    host string,
    ts timestamp,
    cpu double,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('merge_mode'='first_row');

create table if not exists invalid_merge_mode(
    host string,
    ts timestamp,
    cpu double,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('merge_mode'='last_non_null', 'append_mode'='true');
