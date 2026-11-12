-- Query options persist on each protocol's connection. SQLness pins N=4.
-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.parallelism;

-- SQLNESS PROTOCOL MYSQL
SET query_parallelism = 2;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.parallelism;

-- Invalid SET preserves the preceding value.
-- SQLNESS PROTOCOL MYSQL
SET query.parallelism = 0;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.parallelism;

-- SQLNESS PROTOCOL MYSQL
SET query.enable_remote_dynamic_filter_pushdown = false;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.enable_remote_dynamic_filter_pushdown;

-- Restore the shared MySQL connection before another case uses it.
-- SQLNESS PROTOCOL MYSQL
SET query.parallelism = 4;

-- SQLNESS PROTOCOL MYSQL
SET query.enable_remote_dynamic_filter_pushdown = true;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.parallelism;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.enable_remote_dynamic_filter_pushdown;

-- PostgreSQL owns a separate session and accepts the same query options.
-- SQLNESS PROTOCOL POSTGRES
SHOW VARIABLES query.parallelism;

-- SQLNESS PROTOCOL POSTGRES
SET query.parallelism TO 2;

-- SQLNESS PROTOCOL POSTGRES
SHOW VARIABLES query.parallelism;

-- SQLNESS PROTOCOL POSTGRES
SET query.parallelism = 0;

-- SQLNESS PROTOCOL POSTGRES
SHOW VARIABLES query.parallelism;

-- SQLNESS PROTOCOL POSTGRES
SET query.enable_remote_dynamic_filter_pushdown = false;

-- SQLNESS PROTOCOL POSTGRES
SHOW VARIABLES query.enable_remote_dynamic_filter_pushdown;

-- Restore the shared PostgreSQL connection as well.
-- SQLNESS PROTOCOL POSTGRES
SET query.parallelism = 4;

-- SQLNESS PROTOCOL POSTGRES
SET query.enable_remote_dynamic_filter_pushdown = true;

-- SQLNESS PROTOCOL POSTGRES
SHOW VARIABLES query.parallelism;

-- SQLNESS PROTOCOL POSTGRES
SHOW VARIABLES query.enable_remote_dynamic_filter_pushdown;
