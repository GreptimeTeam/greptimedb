--- test information_schema.session_variables ----

USE INFORMATION_SCHEMA;

-- The table is registered with the column names TiDB documents for
-- `information_schema.session_variables`, even though it currently holds no
-- rows. It does not match MySQL exactly: both columns are NOT NULL here.
DESC TABLE SESSION_VARIABLES;

SELECT * FROM SESSION_VARIABLES;

-- A client filtering on a variable name must get an empty result rather than an
-- unknown-table error.
SELECT VARIABLE_NAME, VARIABLE_VALUE
FROM SESSION_VARIABLES
WHERE VARIABLE_NAME = 'sql_mode';

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
