create table if not exists append_mode1(
    host string,
    ts timestamp,
    cpu double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('append_mode'='true');

INSERT INTO append_mode1 VALUES ('host1',0, 0), ('host2', 1, 1,);

INSERT INTO append_mode1 VALUES ('host1',0, 0), ('host2', 1, 1,);

SELECT * from append_mode1 ORDER BY host, ts;

create table if not exists append_mode2(
    host string,
    ts timestamp,
    cpu double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('append_mode'='false');

INSERT INTO append_mode2 VALUES ('host1',0, 0), ('host2', 1, 1,);

INSERT INTO append_mode2 VALUES ('host1',0, 10), ('host2', 1, 11,);

SELECT * from append_mode2 ORDER BY host, ts;

DROP TABLE append_mode1;

DROP TABLE append_mode2;
