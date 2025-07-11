-- Test default values
SELECT @@max_execution_time;

-- Test basic settings
SET MAX_EXECUTION_TIME = 1000; -- Using global variable
SELECT @@max_execution_time;

SET SESSION MAX_EXECUTION_TIME = 2000; -- Using session variable
SELECT @@session.max_execution_time;

-- Test different formats
SET @@SESSION.MAX_EXECUTION_TIME = 3000; -- Using session variable
SELECT @@session.max_execution_time;

SET LOCAL MAX_EXECUTION_TIME = 4000; -- Using local variable
SELECT @@max_execution_time;

-- Test case insensitivity
    -- set
    set max_execution_time = 5000; -- Lowercase
    SELECT @@max_execution_time;

    SET max_EXECUTION_time = 6000; -- Mixed case
    SELECT @@max_execution_time;

    SET MAX_EXECUTION_TIME = 7000; -- Uppercase
    SELECT @@max_execution_time;

    -- select
    SET max_execution_time = 8000;
    SELECT @@max_execution_time; -- Lowercase

    SET max_execution_time = 9000;
    SELECT @@max_Execution_time; -- Mixed case

    SET max_execution_time = 10000;
    SELECT @@MAX_EXECUTION_TIME; -- Uppercase

-- Test the boundary
SET @@max_execution_time = 0; --minimum value for u64
SELECT @@max_execution_time;

SET @@max_execution_time = -1; -- Negative value (not allowed)
SELECT @@max_execution_time;

SET @@max_execution_time = 18446744073709551615; -- Maximum value for u64
SELECT @@max_execution_time;

SET @@max_execution_time = 18446744073709551616; -- Maximum value for u64 + 1 (out of range)
SELECT @@max_execution_time;
