CREATE TABLE phy (ts timestamp time index, val double) engine=metric with ("physical_metric_table" = "");

SHOW TABLES;

DESC TABLE phy;

CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "phy");

CREATE TABLE t2 (ts timestamp time index, job string primary key, val double) engine = metric with ("on_physical_table" = "phy");

SELECT table_catalog, table_schema, table_name, table_type, engine FROM information_schema.tables WHERE engine = 'metric' order by table_name;

DESC TABLE phy;

DESC TABLE t1;

DESC TABLE t2;

-- TODO(ruihang): add a case that drops phy before t1

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
  `QuaErAT` BOOLEAN,
  `REPREHenDERIt` BOOLEAN DEFAULT true,
  `Et` INT NULL,
  `AutEM` INT,
  esse DOUBLE,
  `Tempore` BOOLEAN,
  `reruM` BOOLEAN,
  `eRrOR` BOOLEAN NULL,
  `cOMmodi` BOOLEAN,
  `PERfERENdIS` DOUBLE,
  `eSt` FLOAT DEFAULT 0.70978713,
  PRIMARY KEY(`cOMmodi`, `PERfERENdIS`, esse)
) ENGINE = metric with ("physical_metric_table" = "");

DESC TABLE `auT`;

DROP TABLE `auT`;
