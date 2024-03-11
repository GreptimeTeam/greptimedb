create table if not exists test_opts(
    host string,
    ts timestamp,
    cpu double default 0,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with(regions=1, ttl='7d', 'compaction.twcs.time_window'='1d');

drop table test_opts;

create table if not exists test_opts(
    host string,
    ts timestamp,
    cpu double default 0,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('regions'=1, 'ttl'='7d', 'compaction.twcs.time_window'='1d');

drop table test_opts;
