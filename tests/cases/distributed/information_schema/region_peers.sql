--- test information_schema.region_peers ----
USE INFORMATION_SCHEMA;

CREATE TABLE region_peers_phy (ts timestamp time index, val double) engine = metric with ("physical_metric_table" = "");

CREATE TABLE region_peers_t1 (
    ts timestamp time index,
    val double,
    host string primary key
) engine = metric with ("on_physical_table" = "region_peers_phy");

CREATE TABLE region_peers_t2 (
    ts timestamp time index,
    job string primary key,
    val double
) engine = metric with ("on_physical_table" = "region_peers_phy");

CREATE TABLE region_peers_test (
    a int primary key,
    b string,
    ts timestamp time index,
) PARTITION ON COLUMNS (a) (
    a < 10,
    a >= 10 AND a < 20,
    a >= 20,
);

SELECT region_id,is_leader,status,down_seconds FROM region_peers ORDER BY region_id;

DROP TABLE region_peers_t1, region_peers_t2, region_peers_phy, region_peers_test;

USE public;


