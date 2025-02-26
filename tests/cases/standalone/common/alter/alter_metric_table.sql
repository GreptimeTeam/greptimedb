CREATE TABLE phy (ts timestamp time index, val double) engine=metric with ("physical_metric_table" = "");

SHOW TABLES;

DESC TABLE phy;

CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "phy");

CREATE TABLE t2 (ts timestamp time index, job string primary key, val double) engine = metric with ("on_physical_table" = "phy");

DESC TABLE t1;

DESC TABLE t2;

DESC TABLE phy;

ALTER TABLE t1 ADD COLUMN k STRING PRIMARY KEY;

ALTER TABLE t2 ADD COLUMN k STRING PRIMARY KEY;

DESC TABLE t1;

DESC TABLE t2;

DESC TABLE phy;

DROP TABLE t1;

DROP TABLE t2;

DROP TABLE phy;
