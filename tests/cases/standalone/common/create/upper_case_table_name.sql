create database upper_case_table_name;

use upper_case_table_name;

create table system_Metric(ts timestamp time index);

insert into system_Metric values (0), (1);

select * from system_Metric;

select * from "system_Metric";

drop table system_Metric;

use public;
