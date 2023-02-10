CREATE TABLE monitor ( host STRING, ts TIMESTAMP, cpu DOUBLE DEFAULT 0, memory DOUBLE, TIME INDEX (ts), PRIMARY KEY(host)) ;

insert into monitor(host, cpu, memory, ts) values ('host1', 66.6, 1024, 1655276557000), ('host2', 77.7,  2048, 1655276558000), ('host3', 88.8,  3072, 1655276559000);

select * from monitor;

delete from monitor where host = 'host1' and ts = 1655276557000;

select * from monitor;

drop table monitor;
