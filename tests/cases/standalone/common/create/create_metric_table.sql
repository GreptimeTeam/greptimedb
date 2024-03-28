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
