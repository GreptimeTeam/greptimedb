CREATE TABLE demo_1(host string, cpu double, memory double, ts TIMESTAMP time index);
insert into
    demo_1(host, cpu, memory, ts)
values
    ('host1', 66.6, 1024, 1655276557000),
    ('host2', 88.8, 333.3, 1655276558000),
    ('host3', 99.9, 444.4, 1722077263000);

CREATE TABLE demo_2(host string, cpu double, memory double, ts TIMESTAMP time index);

insert into
    demo_2(host, ts)
values
    ('host4', 1755276557000),
    ('host5', 1755276558000),
    ('host6', 1822077263000);

Copy demo_1 TO '${SQLNESS_HOME}/demo/export/csv_header/demo_1.csv' with (format='csv');

Copy demo_2 TO '${SQLNESS_HOME}/demo/export/csv_header/demo_2.csv' with (format='csv');

CREATE TABLE check_header(host string, cpu double, memory double, ts TIMESTAMP time index);

Copy check_header FROM '${SQLNESS_HOME}/demo/export/csv_header/' with (pattern = 'demo*', format='csv', header='true');

select * from check_header order by ts;

CREATE TABLE non_check_header(host string, cpu double, memory double, ts TIMESTAMP time index);

Copy non_check_header FROM '${SQLNESS_HOME}/demo/export/csv_header/' with (pattern = 'demo*', format='csv', header='false');

select * from non_check_header order by ts;

CREATE TABLE non_continue_error(host string, cpu double, memory double, ts TIMESTAMP time index);

Copy non_continue_error FROM '${SQLNESS_HOME}/demo/export/csv_header/' with (pattern = 'demo*', format='csv', header='true', continue_on_error='false');

drop table demo_1;

drop table demo_2;

drop table check_header;

drop table non_check_header;

drop table non_continue_error;
