-- SQLNESS PROTOCOL MYSQL
SELECt @@tx_isolation;

-- SQLNESS PROTOCOL MYSQL
SELECT @@version_comment;

-- SQLNESS PROTOCOL MYSQL
SHOW DATABASES;

-- ======================================================
-- MySQL compatibility tests for JDBC connectors
-- ======================================================

-- Test MySQL IF() function (issue #7278 compatibility)
-- SQLNESS PROTOCOL MYSQL
SELECT IF(1, 'yes', 'no') as result;

-- SQLNESS PROTOCOL MYSQL
SELECT IF(0, 'yes', 'no') as result;

-- SQLNESS PROTOCOL MYSQL
SELECT IF(NULL, 'yes', 'no') as result;

-- Test IFNULL (should work via DataFusion)
-- SQLNESS PROTOCOL MYSQL
SELECT IFNULL(NULL, 'default') as result;

-- SQLNESS PROTOCOL MYSQL
SELECT IFNULL('value', 'default') as result;

-- Test COALESCE
-- SQLNESS PROTOCOL MYSQL
SELECT COALESCE(NULL, NULL, 'third') as result;

-- Verify SHOW TABLES column naming
-- SQLNESS PROTOCOL MYSQL
USE public;

-- SQLNESS PROTOCOL MYSQL
SHOW TABLES;
