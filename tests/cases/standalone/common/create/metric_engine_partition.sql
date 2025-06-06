create table metric_engine_partition (
    ts timestamp time index,
    host string primary key,
    cpu double,
)
partition on columns (host) (
    host <= 'host1',
    host > 'host1' and host <= 'host2',
    host > 'host2'
)
engine = metric
with (
    physical_metric_table = "true",
);

select count(*) from metric_engine_partition;

create table logical_table_1 (
    ts timestamp time index,
    host string primary key,
    cpu double,
)
partition on columns (host) ()
engine = metric
with (
    on_physical_table = "metric_engine_partition",
);

create table logical_table_2 (
    ts timestamp time index,
    host string primary key,
    cpu double,
)
engine = metric
with (
    on_physical_table = "metric_engine_partition",
);

show create table logical_table_2;

drop table logical_table_2;

drop table metric_engine_partition;
