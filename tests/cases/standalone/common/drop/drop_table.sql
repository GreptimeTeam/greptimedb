DROP TABLE IF EXISTS foo;

create table foo (
     host string,
     ts timestamp DEFAULT '2023-04-29 00:00:00+00:00',
     cpu double default 0,
     TIME INDEX (ts),
     PRIMARY KEY(host)
) engine=mito with(regions=1);

DROP TABLE IF EXISTS foo;

DROP TABLE IF EXISTS foo;
