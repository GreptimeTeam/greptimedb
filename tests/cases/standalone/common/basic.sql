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

DROP TABLE system_metrics;

create table foo (
    host string,
    ts timestamp DEFAULT '2023-04-29 00:00:00+00:00',
    cpu double default 0,
    TIME INDEX (ts),
    PRIMARY KEY(host)
) engine=mito with(regions=1);

insert into foo (host, cpu, ts) values ('host1', 1.1, '2000-01-01 00:00:00+00:00');

insert into foo (host, cpu) values ('host2', 2.2);

insert into foo (host) values ('host3');

select * from foo;

DROP TABLE foo;