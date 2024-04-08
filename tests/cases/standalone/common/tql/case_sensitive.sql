create table "MemAvailable" (ts timestamp time index, instance string primary key, val double);

create table "MemTotal" (ts timestamp time index, instance string primary key, val double);

insert into "MemAvailable" values
    (0, 'host0', 10),
    (5000, 'host0', 20),
    (10000, 'host0', 30),
    (0, 'host1', 40),
    (5000, 'host1', 50),
    (10000, 'host1', 60);

insert into "MemTotal" values
    (0, 'host0', 100),
    (5000, 'host0', 100),
    (10000, 'host0', 100),
    (0, 'host1', 100),
    (5000, 'host1', 100),
    (10000, 'host1', 100);

select table_name from information_schema.tables where table_type = 'BASE TABLE' order by table_id;

-- SQLNESS SORT_RESULT 3 1
tql eval (0,10,'5s') sum(MemAvailable / 4) + sum(MemTotal / 4);

drop table "MemTotal";

create schema "AnotherSchema";

create table "AnotherSchema"."MemTotal" (ts timestamp time index, instance string primary key, val double);

tql eval (0,10,'5s') sum(MemAvailable / 4) + sum(MemTotal / 4);

-- Cross schema is not supported
tql eval (0,10,'5s') sum(MemAvailable / 4) + sum({__name__="AnotherSchema.MemTotal"} / 4);

drop table "MemAvailable";

drop table "AnotherSchema"."MemTotal";

drop schema "AnotherSchema";
