CREATE TABLE phy (ts timestamp time index, val double) engine=metric with ("physical_metric_table" = "");

CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "phy");

INSERT INTO t1 VALUES ('host1',0, 0), ('host2', 1, 1,);

SELECT * from t1;

CREATE TABLE t2 (ts timestamp time index, job string primary key, val double) engine = metric with ("on_physical_table" = "phy");

SELECT * from t2;

INSERT INTO t2 VALUES ('job1', 0, 0), ('job2', 1, 1);

SELECT * from t2;

DROP TABLE t1;

DROP TABLE t2;

DESC TABLE phy;

SELECT ts, val, __tsid, host, job FROM phy;

DROP TABLE phy;
