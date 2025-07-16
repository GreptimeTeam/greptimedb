-- Test default values
-- SQLNESS PROTOCOL MYSQL
SELECT @@max_execution_time;

-- Test basic settings
 -- Using global variable
-- SQLNESS PROTOCOL MYSQL
SET MAX_EXECUTION_TIME = 1000;
-- SQLNESS PROTOCOL MYSQL
SELECT @@max_execution_time;

-- Using session variable
-- SQLNESS PROTOCOL MYSQL
SET SESSION MAX_EXECUTION_TIME = 2000;
-- SQLNESS PROTOCOL MYSQL
SELECT @@session.max_execution_time;

-- Test different formats
-- Using session variable
-- SQLNESS PROTOCOL MYSQL
SET @@SESSION.MAX_EXECUTION_TIME = 3000;
-- SQLNESS PROTOCOL MYSQL
SELECT @@session.max_execution_time;

-- Using local variable
-- SQLNESS PROTOCOL MYSQL
SET LOCAL MAX_EXECUTION_TIME = 4000;
-- SQLNESS PROTOCOL MYSQL
SELECT @@max_execution_time;

-- Test case insensitivity
    -- set
    -- Lowercase
    -- SQLNESS PROTOCOL MYSQL
    set max_execution_time = 5000;
    -- SQLNESS PROTOCOL MYSQL
    SELECT @@max_execution_time;

    -- Mixed case
    -- SQLNESS PROTOCOL MYSQL
    SET max_EXECUTION_time = 6000;

    -- SQLNESS PROTOCOL MYSQL
    SELECT @@max_execution_time;

    -- Uppercase
    -- SQLNESS PROTOCOL MYSQL
    SET MAX_EXECUTION_TIME = 7000;
    -- SQLNESS PROTOCOL MYSQL
    SELECT @@max_execution_time;

    -- select
    -- Lowercase
    -- SQLNESS PROTOCOL MYSQL
    SET max_execution_time = 8000;
    -- SQLNESS PROTOCOL MYSQL
    SELECT @@max_execution_time;

    -- Mixed case
    -- SQLNESS PROTOCOL MYSQL
    SET max_execution_time = 9000;
    -- SQLNESS PROTOCOL MYSQL
    SELECT @@max_Execution_time;

    -- Uppercase
    -- SQLNESS PROTOCOL MYSQL
    SET max_execution_time = 10000;
    -- SQLNESS PROTOCOL MYSQL
    SELECT @@MAX_EXECUTION_TIME;

-- Test the boundary
--minimum value for u64
-- SQLNESS PROTOCOL MYSQL
SET @@max_execution_time = 0;
-- SQLNESS PROTOCOL MYSQL
SELECT @@max_execution_time;

-- Negative value (not allowed)
-- SQLNESS PROTOCOL MYSQL
SET @@max_execution_time = -1;
-- SQLNESS PROTOCOL MYSQL
SELECT @@max_execution_time;

-- Maximum value for u64
-- SQLNESS PROTOCOL MYSQL
SET @@max_execution_time = 18446744073709551615;
-- SQLNESS PROTOCOL MYSQL
SELECT @@max_execution_time;

-- Maximum value for u64 + 1 (out of range)
-- SQLNESS PROTOCOL MYSQL
SET @@max_execution_time = 18446744073709551616;
-- SQLNESS PROTOCOL MYSQL
SELECT @@max_execution_time;
