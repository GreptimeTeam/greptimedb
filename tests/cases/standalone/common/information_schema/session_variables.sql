--- test information_schema.session_variables ----

USE INFORMATION_SCHEMA;

-- The table reports the session's own state, using the column names TiDB
-- documents for `information_schema.session_variables`.
DESC TABLE SESSION_VARIABLES;

-- Defaults before anything is set on the session.
SELECT VARIABLE_NAME, VARIABLE_VALUE
FROM SESSION_VARIABLES
ORDER BY VARIABLE_NAME;

-- `SET` on the same connection is reflected in the table, and agrees with the
-- `@@` lookup that already worked. MySQL is the channel that supports
-- `max_execution_time`, and it keeps one session across statements.
-- SQLNESS PROTOCOL MYSQL
SET SESSION max_execution_time = 2000;

-- SQLNESS PROTOCOL MYSQL
SELECT @@session.max_execution_time;

-- SQLNESS PROTOCOL MYSQL
SELECT VARIABLE_VALUE
FROM INFORMATION_SCHEMA.SESSION_VARIABLES
WHERE VARIABLE_NAME = 'max_execution_time';

-- SQLNESS PROTOCOL MYSQL
SET SESSION time_zone = 'Asia/Shanghai';

-- SQLNESS PROTOCOL MYSQL
SELECT VARIABLE_VALUE
FROM INFORMATION_SCHEMA.SESSION_VARIABLES
WHERE VARIABLE_NAME = 'time_zone';

-- SQLNESS PROTOCOL MYSQL
SET SESSION skip_wal = 'true';

-- SQLNESS PROTOCOL MYSQL
SELECT VARIABLE_VALUE
FROM INFORMATION_SCHEMA.SESSION_VARIABLES
WHERE VARIABLE_NAME = 'skip_wal';

-- MySQL compatibility values come from the same definitions the protocol
-- answers `@@` from, so the table and the protocol agree. Before this shared
-- definition existed, `@@sql_mode` reported `0` while `SHOW VARIABLES` reported
-- the real mode; both now report the real mode.
-- SQLNESS PROTOCOL MYSQL
SELECT @@sql_mode;

-- SQLNESS PROTOCOL MYSQL
SELECT VARIABLE_VALUE
FROM INFORMATION_SCHEMA.SESSION_VARIABLES
WHERE VARIABLE_NAME = 'sql_mode';

-- SQLNESS PROTOCOL MYSQL
SELECT @@wait_timeout;

-- SQLNESS PROTOCOL MYSQL
SELECT VARIABLE_VALUE
FROM INFORMATION_SCHEMA.SESSION_VARIABLES
WHERE VARIABLE_NAME = 'wait_timeout';

-- `query.*` options report the engine's effective value, so an option the
-- session never set shows its default instead of going missing.
SELECT VARIABLE_NAME, VARIABLE_VALUE
FROM SESSION_VARIABLES
WHERE VARIABLE_NAME = 'query.parallelism';

-- A variable the table does not report yields no row rather than an error.
SELECT VARIABLE_NAME, VARIABLE_VALUE
FROM SESSION_VARIABLES
WHERE VARIABLE_NAME = 'not_a_variable';

-- The table is listed among the information_schema tables.
SELECT table_schema, table_name
FROM TABLES
WHERE table_schema = 'information_schema' AND table_name = 'session_variables';

-- Both columns are registered in `information_schema.columns`.
SELECT column_name, data_type, is_nullable
FROM COLUMNS
WHERE table_schema = 'information_schema' AND table_name = 'session_variables'
ORDER BY column_name;

use public;
