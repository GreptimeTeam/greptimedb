create database upper_case_table_name;

use upper_case_table_name;

create table "system_Metric"(ts timestamp time index);

insert into "system_Metric" values (0), (1);

select * from system_Metric;

select * from "system_Metric";

drop table "system_Metric";

create table "AbCdEfG"("CoLA" string, "cOlB" string, "tS" timestamp time index, primary key ("CoLA"));

desc table "AbCdEfG";

-- unquoted table name and column name.
create table AbCdEfGe(CoLA string, cOlB string, tS timestamp time index, primary key (cOlA));

desc table aBcDeFgE;

drop table "AbCdEfG";

drop table aBcDeFgE;

-- unquoted column name in partition
create table AbCdEfGe(
    CoLA string PRIMARY KEY,
    tS timestamp time index
) PARTITION BY RANGE COLUMNS (cOlA) (
    PARTITION p0 VALUES LESS THAN (MAXVALUE)
);

drop table abcdefge;

-- unquoted column name in TIME INDEX
create table AbCdEfGe(CoLA string, tS timestamp, TIME INDEX (Ts));

desc table abcdefge;

drop table abcdefge;

use public;
