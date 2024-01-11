create table data (ts timestamp(3) time index, val double);

insert into data values (0, 1), (10000, 2), (20000, 3);

tql eval (0, 30, '10s'), data < 1;

tql eval (0, 30, '10s'), data + (1 < bool 2);

tql eval (0, 30, '10s'), data + (1 > bool 2);

drop table data;

-- Binary operator on table with multiple field columns

create table data (ts timestamp time index, val1 double, val2 double, val3 double);

insert into data values (0, 1, 100, 10000), (10000, 2, 200, 20000), (20000, 3, 300, 30000);

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 30, '10s'), data / data;

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 30, '10s'), data{__field__="val1"} + data{__field__="val2"};

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 30, '10s'), data{__field__="val1", __field__="val2"} + data{__field__="val2", __field__="val3"};

drop table data;

create table host_cpu_seconds_total (
    ts timestamp time index,
    val double,
    host string,
    `mode` string,
    primary key (host, `mode`)
);

insert into host_cpu_seconds_total values
    (0, 0.1, 'host1', 'idle'),
    (0, 0.2, 'host1', 'user'),
    (0, 0.3, 'host1', 'system'),
    (10000, 0.4, 'host1', 'idle'),
    (10000, 0.5, 'host1', 'user'),
    (10000, 0.6, 'host1', 'system'),
    (20000, 0.2, 'host1', 'idle'),
    (20000, 0.3, 'host1', 'user'),
    (20000, 0.4, 'host1', 'system'),
    (30000, 0.5, 'host1', 'idle'),
    (30000, 0.6, 'host1', 'user'),
    (30000, 0.7, 'host1', 'system');

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 30, '10s') (sum by(host) (irate(host_cpu_seconds_total{mode!="idle"}[1m0s])) / sum by (host)((irate(host_cpu_seconds_total[1m0s])))) * 100;

drop table host_cpu_seconds_total;
