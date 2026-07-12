create table host_load1(
    ts timestamp time index,
    collector string,
    host string,
    val double,
    primary key (collector, host)
);

describe table host_load1;

describe host_load1;

desc table host_load1;

desc host_load1;

drop table host_load1;
