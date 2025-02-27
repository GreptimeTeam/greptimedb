
create table cache_hit (
    ts timestamp time index,
    job string,
    greptime_value double,
    primary key (job)
);

insert into cache_hit values
    (3000, "read", 123.45),
    (3000, "write", 234.567),
    (4000, "read", 345.678),
    (4000, "write", 456.789);

-- SQLNESS SORT_RESULT 3 1
tql eval (3, 4, '1s') round(cache_hit, 0.01);

-- SQLNESS SORT_RESULT 3 1
tql eval (3, 4, '1s') round(cache_hit, 0.1);

-- SQLNESS SORT_RESULT 3 1
tql eval (3, 4, '1s') round(cache_hit, 1.0);

-- SQLNESS SORT_RESULT 3 1
tql eval (3, 4, '1s') round(cache_hit);

-- SQLNESS SORT_RESULT 3 1
tql eval (3, 4, '1s') round(cache_hit, 10.0);

drop table cache_hit;
