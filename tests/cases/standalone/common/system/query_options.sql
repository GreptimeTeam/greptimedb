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

-- Native DataFusion options are accepted through MySQL SET and SHOW.
-- SQLNESS PROTOCOL MYSQL
SET datafusion.optimizer.prefer_hash_join = false;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES datafusion.optimizer.prefer_hash_join;

-- SQLNESS PROTOCOL MYSQL
SET query.enable_remote_dynamic_filter_pushdown = false;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.enable_remote_dynamic_filter_pushdown;

-- Invalid query parallelism is rejected at both bounds.
-- SQLNESS PROTOCOL MYSQL
SET query.parallelism = 0;

-- SQLNESS PROTOCOL MYSQL
SET query.parallelism = 1025;

-- Native DataFusion options are accepted through PostgreSQL SET and SHOW.
-- SQLNESS PROTOCOL POSTGRES
SET datafusion.optimizer.enable_dynamic_filter_pushdown TO 'FALSE';

-- SQLNESS PROTOCOL POSTGRES
SHOW datafusion.optimizer.enable_dynamic_filter_pushdown;

-- SQLNESS PROTOCOL POSTGRES
SET query.allow_query_fallback TO true;

-- SQLNESS PROTOCOL POSTGRES
SHOW query.allow_query_fallback;

-- PostgreSQL query-option typos are errors.
-- SQLNESS PROTOCOL POSTGRES
SET query.unknown_option TO true;
