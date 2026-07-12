-- Test unsupported set variables for MySQL protocol
-- These should succeed with a warning instead of failing

-- Test setting an unsupported variable
-- SQLNESS PROTOCOL MYSQL
SET autocommit = 1;

-- Test setting with @@ prefix (previously this would succeed)
-- SQLNESS PROTOCOL MYSQL
SET @@autocommit = 1;

-- Test setting character_set_client (commonly used by MySQL clients)
-- SQLNESS PROTOCOL MYSQL
SET character_set_client = 'utf8mb4';

-- Test setting character_set_results
-- SQLNESS PROTOCOL MYSQL
SET character_set_results = 'utf8mb4';

-- Test setting sql_mode
-- SQLNESS PROTOCOL MYSQL
SET sql_mode = 'STRICT_TRANS_TABLES';

-- Test multiple unsupported settings
-- SQLNESS PROTOCOL MYSQL
SET @@session.sql_mode = 'TRADITIONAL';

-- Test NAMES (special MySQL syntax for character set)
-- SQLNESS PROTOCOL MYSQL
SET NAMES utf8mb4;

-- Test collation_connection
-- SQLNESS PROTOCOL MYSQL
SET collation_connection = 'utf8mb4_unicode_ci';

-- Test SHOW WARNINGS after setting unsupported variable
-- SQLNESS PROTOCOL MYSQL
SET some_unsupported_var = 123;

-- SQLNESS PROTOCOL MYSQL
SHOW WARNINGS;

-- Test that warning is cleared after next statement
-- SQLNESS PROTOCOL MYSQL
SELECT 1;

-- SQLNESS PROTOCOL MYSQL
SHOW WARNINGS;
