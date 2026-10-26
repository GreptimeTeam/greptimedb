-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.allow_query_fallback;
-- SQLNESS PROTOCOL MYSQL
SET query.allow_query_fallback = true;
-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.allow_query_fallback;

-- SQLNESS PROTOCOL POSTGRES
SHOW query.allow_query_fallback;
-- SQLNESS PROTOCOL POSTGRES
SET query.allow_query_fallback = true;
-- SQLNESS PROTOCOL POSTGRES
SHOW query.allow_query_fallback;

SELECT current_schema();
DROP DATABASE IF EXISTS session_isolation_a;
CREATE DATABASE IF NOT EXISTS session_isolation_a;
USE session_isolation_a;
SELECT current_schema();
SELECT timezone();
SET time_zone = '+08:00';
SELECT timezone();
