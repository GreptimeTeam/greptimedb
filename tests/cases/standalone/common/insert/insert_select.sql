create table demo1(host string, cpu double, memory double, ts timestamp time index);

create table demo2(host string, cpu double, memory double, ts timestamp time index);

insert into demo1(host, cpu, memory, ts) values ('host1', 66.6, 1024, 1655276557000), ('host2', 88.8,  333.3, 1655276558000);

insert into demo2(host) select * from demo1;

insert into demo2 select cpu,memory from demo1;

insert into demo2(ts) select memory from demo1;

insert into demo2 select * from demo1;

select * from demo2 order by ts;

drop table demo1;

drop table demo2;
