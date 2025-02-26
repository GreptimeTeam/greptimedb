--- test information_schema.procedure_info ----
USE public;

CREATE TABLE procedure_info_for_sql_test1(
  ts TIMESTAMP TIME INDEX,
  temperature DOUBLE DEFAULT 10,
) engine=mito with('append_mode'='true');

CREATE TABLE procedure_info_for_sql_test2(
  ts TIMESTAMP TIME INDEX,
  temperature DOUBLE DEFAULT 10,
) engine=mito with('append_mode'='true');

use INFORMATION_SCHEMA;

select procedure_type from procedure_info where lock_keys like '%procedure_info_for_sql_test%';

use public;

DROP TABLE procedure_info_for_sql_test1, procedure_info_for_sql_test2;
