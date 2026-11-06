-- Partial inspection is opt-in and session-scoped on both SQL protocols.
-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.allow_partial_inspection_results;

-- SQLNESS PROTOCOL MYSQL
SET query.allow_partial_inspection_results = true;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.allow_partial_inspection_results;

-- The PostgreSQL connection has independent defaults.
-- SQLNESS PROTOCOL POSTGRES
SHOW VARIABLES query.allow_partial_inspection_results;

-- SQLNESS PROTOCOL POSTGRES
SET query.allow_partial_inspection_results = true;

-- SQLNESS PROTOCOL POSTGRES
SHOW VARIABLES query.allow_partial_inspection_results;

-- SQLNESS PROTOCOL MYSQL
SET query.allow_partial_inspection_results = 'invalid';

-- Invalid input must preserve the previous value.
-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES query.allow_partial_inspection_results;

-- SQLNESS PROTOCOL MYSQL
SELECT 1;

-- SQLNESS PROTOCOL MYSQL
SET query.allow_partial_inspection_results = false;

-- SQLNESS PROTOCOL POSTGRES
SET query.allow_partial_inspection_results = false;
