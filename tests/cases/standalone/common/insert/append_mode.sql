create table if not exists append_mode_on(
    host string,
    ts timestamp,
    cpu double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('append_mode'='true');

INSERT INTO append_mode_on VALUES ('host1',0, 0), ('host2', 1, 1,);

INSERT INTO append_mode_on VALUES ('host1',0, 0), ('host2', 1, 1,);

SELECT * from append_mode_on ORDER BY host, ts;

create table if not exists append_mode_off(
    host string,
    ts timestamp,
    cpu double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('append_mode'='false');

INSERT INTO append_mode_off VALUES ('host1',0, 0), ('host2', 1, 1,);

INSERT INTO append_mode_off VALUES ('host1',0, 10), ('host2', 1, 11,);

SELECT * from append_mode_off ORDER BY host, ts;

DROP TABLE append_mode_on;

DROP TABLE append_mode_off;
