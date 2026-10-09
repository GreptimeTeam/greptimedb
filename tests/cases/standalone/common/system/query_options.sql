-- Query options are session-scoped; request hints take precedence per query.
-- SQLNESS PROTOCOL MYSQL
SET query_parallelism = 8;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.parallelism;

-- SQLNESS PROTOCOL MYSQL
SET query.allow_query_fallback = false;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.allow_query_fallback;

-- Query namespace typos and unapproved DataFusion options are errors.
-- SQLNESS PROTOCOL MYSQL
SET query.unknown_option = true;

-- SQLNESS PROTOCOL MYSQL
SET datafusion.execution.batch_size = 128;

-- Computed expressions are not accepted for query options.
-- SQLNESS PROTOCOL MYSQL
SET query.parallelism = 4 + 4;

-- Third-party startup variables remain compatible.
-- SQLNESS PROTOCOL MYSQL
SET autocommit = 1;

-- SQLNESS PROTOCOL POSTGRES
SET QUERY.PARALLELISM TO 16;

-- SQLNESS PROTOCOL POSTGRES
SHOW QUERY.PARALLELISM;
