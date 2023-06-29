create table data (ts timestamp(3) time index, val double);

insert into data values (0, 1), (10000, 2), (20000, 3);

tql eval (0, 30, '10s'), data < 1;

tql eval (0, 30, '10s'), data + (1 < bool 2);

tql eval (0, 30, '10s'), data + (1 > bool 2);

drop table data;
