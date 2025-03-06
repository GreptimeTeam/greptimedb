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

CREATE TABLE phy (
  ts timestamp time index,
  val double,
  host string primary key
) 
PARTITION ON COLUMNS ("host") (
   host < '1024',
   host >= '1024'
)
engine=metric
with ("physical_metric_table" = "");

CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine=metric with ("on_physical_table" = "phy");

INSERT INTO t1 (ts, val, host) VALUES 
  ('2022-01-01 00:00:00', 1.23, 'example.com'),
  ('2022-01-02 00:00:00', 4.56, 'example.com'),
  ('2022-01-03 00:00:00', 7.89, 'example.com'),
  ('2022-01-01 00:00:00', 1.23, 'example.com'),
  ('2022-01-02 00:00:00', 4.56, 'example.com'),
  ('2022-01-03 00:00:00', 7.89, 'example.com');

SELECT * FROM t1;

ALTER TABLE t1 ADD COLUMN k STRING PRIMARY KEY;

SELECT * FROM t1;

DROP TABLE t1;

DROP TABLE phy;
