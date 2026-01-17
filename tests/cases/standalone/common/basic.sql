USE public;

CREATE TABLE system_metrics (
    host STRING,
    idc STRING,
    cpu_util DOUBLE,
    memory_util DOUBLE,
    disk_util DOUBLE,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY(host, idc),
    TIME INDEX(ts)
);

INSERT INTO system_metrics
VALUES
    ("host1", "idc_a", 11.8, 10.3, 10.3, 1667446797450),
    ("host2", "idc_a", 80.0, 70.3, 90.0, 1667446797450),
    ("host1", "idc_b", 50.0, 66.7, 40.6, 1667446797450);

SELECT * FROM system_metrics;

SELECT count(*) FROM system_metrics;

SELECT avg(cpu_util) FROM system_metrics;

SELECT idc, avg(memory_util) FROM system_metrics GROUP BY idc ORDER BY idc;

create table foo (
    host string,
    ts timestamp DEFAULT '2023-04-29 00:00:00+00:00',
    cpu double default 0,
    TIME INDEX (ts),
    PRIMARY KEY(host)
) engine=mito;

insert into foo (host, cpu, ts) values ('host1', 1.1, '2000-01-01 00:00:00+00:00');

insert into foo (host, cpu) values ('host2', 2.2);

insert into foo (host) values ('host3');

select * from foo order by ts;

CREATE TABLE phy (ts timestamp time index, val double) engine=metric with ("physical_metric_table" = "");

CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "phy");

INSERT INTO t1 VALUES ('host1',0, 0), ('host2', 1, 1,);

SELECT * from t1;

delete from t1;

-- do not support DELETE FROM physical table for now
delete from phy;

CREATE TABLE t2 (ts timestamp time index, job string primary key, val double) engine = metric with ("on_physical_table" = "phy");

SELECT * from t2;

INSERT INTO t2 VALUES ('job1', 0, 0), ('job2', 1, 1);

-- SQLNESS ARG restart=true

SELECT * FROM system_metrics;

select * from foo order by host asc;

SELECT * from t1 order by ts desc;

SELECT * from t2 order by ts desc;

DROP TABLE t1;

DROP TABLE t2;

DROP TABLE phy;

DROP TABLE system_metrics;

DROP TABLE foo;

-- SQLNESS PROTOCOL MYSQL
SET MAX_EXECUTION_TIME = 2000;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES MAX_EXECUTION_TIME;

-- SQLNESS PROTOCOL MYSQL
SET MAX_EXECUTION_TIME = 0;
