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

-- Value splitting is disabled for last_non_null so the two physical value
-- columns cannot be merged independently into a stale logical value.
CREATE TABLE phy_last_non_null (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE
) ENGINE = metric WITH (
    "physical_metric_table" = "",
    "merge_mode" = "last_non_null"
);

CREATE TABLE metric_last_non_null (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    host STRING PRIMARY KEY
) ENGINE = metric WITH ("on_physical_table" = "phy_last_non_null");

INSERT INTO metric_last_non_null VALUES ('host1', 1, 1.0);
ADMIN flush_table('phy_last_non_null');
INSERT INTO metric_last_non_null VALUES ('host1', 1, 1.5);

SELECT host, ts, val FROM metric_last_non_null;

ADMIN flush_table('phy_last_non_null');

SELECT host, ts, val FROM metric_last_non_null;
DESC TABLE phy_last_non_null;

DROP TABLE metric_last_non_null;
DROP TABLE phy_last_non_null;

CREATE TABLE phy_default (ts timestamp time index, val double default 42) engine=metric with ("physical_metric_table" = "");

CREATE TABLE t_default (ts timestamp time index, val double default 42, host string primary key) engine = metric with ("on_physical_table" = "phy_default");

INSERT INTO t_default (host, ts) VALUES ('host1', 0), ('host2', 1);

SELECT host, ts, val FROM t_default ORDER BY host;

-- SQLNESS REPLACE (region\s\d+\(\d+\,\s\d+\)) region
INSERT INTO t_default (host, val) VALUES ('host3', 3);

DROP TABLE t_default;

DROP TABLE phy_default;

CREATE TABLE phy (
    ts timestamp time index,
    val double
) engine = metric with (
    "physical_metric_table" = "",
    "memtable.type" = "bulk",
    "primary_key_encoding" = "sparse"
);

CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "phy");

INSERT INTO t1 VALUES ('host1',0, 0), ('host2', 1, 1,);

SELECT * from t1;

CREATE TABLE t2 (ts timestamp time index, job string primary key, val double) engine = metric with ("on_physical_table" = "phy");

SELECT * from t2;

INSERT INTO t2 VALUES ('job1', 0, 0), ('job2', 1, 1);

SELECT * from t2;

ADMIN flush_table('phy');

-- SQLNESS ARG restart=true
INSERT INTO t2 VALUES ('job3', 0, 0), ('job4', 1, 1);

SELECT * from t1;

SELECT * from t2;

DROP TABLE t1;

DROP TABLE t2;

DESC TABLE phy;

DROP TABLE phy;
