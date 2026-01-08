-- Test CSV import optional parameter `header` functionality
-- First, create tables and export data into csv files as test files
CREATE TABLE demo_1(host string, cpu double, memory double, jsons JSON, ts TIMESTAMP time index);
insert into
    demo_1(host, cpu, memory, jsons, ts)
values
    ('host1', 66.6, 1024, '{"host1":"1"}', 1655276557000),
    ('host2', 88.8, 333.3, '{"host2":"2"}', 1655276558000),
    ('host3', 99.9, 444.4, '{"host3":"3"}', 1722077263000);

CREATE TABLE demo_2(host string, jsons JSON, ts TIMESTAMP time index);

insert into
    demo_2(host, jsons, ts)
values
    ('host4', '{"host4":"4"}', 1755276557000),
    ('host5', '{"host5":"5"}', 1755276558000),
    ('host6', '{"host6":"6"}', 1822077263000);

Copy demo_1 TO '${SQLNESS_HOME}/demo/export/csv_header/demo_1.csv' with (format='csv');

Copy demo_2 TO '${SQLNESS_HOME}/demo/export/csv_header/demo_2.csv' with (format='csv');

-- Test the case where a CSV has more fields than the table. (header=true, continue_on_error=false) (table: 4, file: 5)

CREATE TABLE enable_long_fields(host string, cpu double, jsons JSON, ts TIMESTAMP time index);

Copy enable_long_fields FROM '${SQLNESS_HOME}/demo/export/csv_header/demo_1.csv' with (format='csv', header='true', continue_on_error='false');

SELECT count(*) FROM enable_long_fields;

-- Test the number of CSV fields is fewer than the table’s. (header=true, continue_on_error=false) (table: 5, file: 3)

CREATE TABLE enable_short_fields(host string, cpu double, memory double, jsons JSON, ts TIMESTAMP time index);

Copy enable_short_fields FROM '${SQLNESS_HOME}/demo/export/csv_header/demo_2.csv' with (format='csv', header='true', continue_on_error='false');

SELECT count(*) FROM enable_short_fields;

-- Test the case where a CSV has more fields than the table. (header=false) (table: 4, file: 5)

CREATE TABLE disable_long_fields(host string, cpu double, jsons JSON, ts TIMESTAMP time index);

Copy disable_long_fields FROM '${SQLNESS_HOME}/demo/export/csv_header/demo_1.csv' with (format='csv', header='false');

SELECT count(*) FROM disable_long_fields;

SELECT * FROM disable_long_fields order by ts;

-- Test the number of CSV fields is fewer than the table’s. (header=false) (table: 5, file: 3)

CREATE TABLE disable_short_fields(host string, cpu double, memory double, jsons JSON, ts TIMESTAMP time index);

Copy disable_short_fields FROM '${SQLNESS_HOME}/demo/export/csv_header/demo_2.csv' with (format='csv', header='false');

SELECT count(*) FROM disable_short_fields;

SELECT * FROM disable_short_fields order by ts;

-- Test the order of CSV fields differs from the table’s. (header=true, continue_on_error=false)

CREATE TABLE enable_different_order(host string, memory double, jsons JSON, cpu double, ts TIMESTAMP time index);

Copy enable_different_order FROM '${SQLNESS_HOME}/demo/export/csv_header/demo_1.csv' with (format='csv', header='true', continue_on_error='false');

SELECT count(*) FROM enable_different_order;

-- Test the order of CSV fields differs from the table’s. (header=false)

CREATE TABLE disable_different_order(host string, memory double, jsons JSON, cpu double, ts TIMESTAMP time index);

Copy disable_different_order FROM '${SQLNESS_HOME}/demo/export/csv_header/demo_1.csv' with (format='csv', header='false');

SELECT count(*) FROM disable_different_order;

-- Test the order of CSV fields differs from the table’s. (header=true, continue_on_error=false)

CREATE TABLE different_order(host string, memory double, jsons JSON, cpu double, ts TIMESTAMP time index);

Copy different_order FROM '${SQLNESS_HOME}/demo/export/csv_header/demo_1.csv' with (format='csv', header='true', continue_on_error='false');

SELECT count(*) FROM different_order;

-- Test to validate that the header schema of the file matches the table schema
-- Import only the files that successfully pass the checks

CREATE TABLE check_header(host string, cpu double, memory double, jsons JSON, ts TIMESTAMP time index);

Copy check_header FROM '${SQLNESS_HOME}/demo/export/csv_header/' with (pattern = 'demo*', format='csv', header='true', continue_on_error='false');

SELECT count(*) from check_header;

CREATE TABLE check_header_continue_error(host string, cpu double, memory double, jsons JSON, ts TIMESTAMP time index);

Copy check_header_continue_error FROM '${SQLNESS_HOME}/demo/export/csv_header/' with (pattern = 'demo*', format='csv', header='true', continue_on_error='true');

SELECT count(*) from check_header_continue_error;

-- Test to skip the header validation

CREATE TABLE non_check_header(host string, cpu double, memory double, jsons JSON, ts TIMESTAMP time index);

Copy non_check_header FROM '${SQLNESS_HOME}/demo/export/csv_header/' with (pattern = 'demo*', format='csv', header='false');

SELECT * from non_check_header order by ts;

drop table demo_1;

drop table demo_2;

drop table enable_long_fields;

drop table enable_short_fields;

drop table disable_long_fields;

drop table disable_short_fields;

drop table enable_different_order;

drop table disable_different_order;

drop table different_order;

drop table check_header;

drop table check_header_continue_error;

drop table non_check_header;
