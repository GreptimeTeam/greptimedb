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
