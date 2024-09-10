CREATE TABLE phy (ts timestamp time index, val double) engine=metric with ("physical_metric_table" = "");

SHOW TABLES;

DESC TABLE phy;

-- create table with duplicate column def
CREATE TABLE t1(ts timestamp time index, val double, host text, host string) engine=metric with ("on_physical_table" = "phy");

CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "phy");

CREATE TABLE t2 (ts timestamp time index, job string primary key, val double) engine = metric with ("on_physical_table" = "phy");

-- create logical table with different data type on field column
CREATE TABLE t3 (ts timestamp time index, val string, host string, primary key (host)) engine=metric with ("on_physical_table" = "phy");

-- create logical table with different data type on tag column
CREATE TABLE t4 (ts timestamp time index, val double, host double, primary key (host)) engine=metric with ("on_physical_table" = "phy");

-- create logical table with different column name on field column
CREATE TABLE t5 (ts timestamp time index, valval double, host string primary key) engine = metric with ("on_physical_table" = "phy");

SELECT table_catalog, table_schema, table_name, table_type, engine FROM information_schema.tables WHERE engine = 'metric' order by table_name;

DESC TABLE phy;

DESC TABLE t1;

DESC TABLE t2;

-- should be failed
-- SQLNESS REPLACE (region\s\d+\(\d+\,\s\d+\)) region
DROP TABLE phy;
-- metadata should be restored
DESC TABLE phy;

DROP TABLE t1;

DROP TABLE t2;

DROP TABLE phy;

-- create one with other primary keys
CREATE TABLE phy2 (ts timestamp time index, val double, abc string, def string, primary key (abc, def)) engine=metric with ("physical_metric_table" = "");

DESC TABLE phy2;

DROP TABLE phy2;

-- fuzz test case https://github.com/GreptimeTeam/greptimedb/issues/3612
CREATE TABLE `auT`(
  incidunt TIMESTAMP(3) TIME INDEX,
  `REPREHenDERIt` double DEFAULT 0.70978713,
  `cOMmodi` STRING,
  `PERfERENdIS` STRING,
  PRIMARY KEY(`cOMmodi`, `PERfERENdIS`)
) ENGINE = metric with ("physical_metric_table" = "");

DESC TABLE `auT`;

DROP TABLE `auT`;

-- append-only metric table
CREATE TABLE
  phy (ts timestamp time index, val double) engine = metric
with
(
  "physical_metric_table" = "",
  "append_mode" = "true"
);

CREATE TABLE t1(ts timestamp time index, val double, host string primary key) engine=metric with ("on_physical_table" = "phy");

INSERT INTO t1 (ts, val, host) VALUES 
  ('2022-01-01 00:00:00', 1.23, 'example.com'),
  ('2022-01-02 00:00:00', 4.56, 'example.com'),
  ('2022-01-03 00:00:00', 7.89, 'example.com'),
  ('2022-01-01 00:00:00', 1.23, 'example.com'),
  ('2022-01-02 00:00:00', 4.56, 'example.com'),
  ('2022-01-03 00:00:00', 7.89, 'example.com');

SELECT * FROM t1;

DROP TABLE t1;

DESC TABLE t1;

DROP TABLE phy;
