-- Each protocol reuses its own connection across requests.
-- SQLNESS PROTOCOL MYSQL
SELECT procedure_state();

-- SQLNESS PROTOCOL POSTGRES
SELECT procedure_state();

CREATE TABLE procedure_state_session (ts TIMESTAMP TIME INDEX);

-- Synchronous DDL must not record a Procedure ID in the Session.
-- SQLNESS PROTOCOL MYSQL
TRUNCATE TABLE procedure_state_session WITH (wait=true);

-- SQLNESS PROTOCOL MYSQL
SELECT procedure_state();

-- SQLNESS PROTOCOL POSTGRES
TRUNCATE TABLE procedure_state_session WITH (wait=true);

-- SQLNESS PROTOCOL POSTGRES
SELECT procedure_state();

-- Synchronous DDL preserves the ID returned by the previous request.
-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE [0-9a-f]{8}-[0-9a-f-]{27} PROC_ID
TRUNCATE TABLE procedure_state_session WITH (wait=false);

-- SQLNESS PROTOCOL MYSQL
TRUNCATE TABLE procedure_state_session WITH (wait=true);

-- SQLNESS PROTOCOL MYSQL
SELECT procedure_state() IN ('{"status":"Running"}', '{"status":"Done"}') AS has_state;

-- A different physical connection still has no history.
-- SQLNESS PROTOCOL POSTGRES
SELECT procedure_state();

-- SQLNESS PROTOCOL POSTGRES
-- SQLNESS REPLACE [0-9a-f]{8}-[0-9a-f-]{27} PROC_ID
TRUNCATE TABLE procedure_state_session WITH (wait=false);

-- SQLNESS PROTOCOL POSTGRES
TRUNCATE TABLE procedure_state_session WITH (wait=true);

-- SQLNESS PROTOCOL POSTGRES
SELECT procedure_state() IN ('{"status":"Running"}', '{"status":"Done"}') AS has_state;

-- Failed submissions must preserve history.
-- SQLNESS PROTOCOL MYSQL
TRUNCATE TABLE nonexistent_session_table WITH (wait=false);

-- SQLNESS PROTOCOL MYSQL
SELECT procedure_state() IN ('{"status":"Running"}', '{"status":"Done"}') AS has_state;

-- SQLNESS PROTOCOL POSTGRES
TRUNCATE TABLE nonexistent_session_table WITH (wait=false);

-- SQLNESS PROTOCOL POSTGRES
SELECT procedure_state() IN ('{"status":"Running"}', '{"status":"Done"}') AS has_state;

DROP TABLE procedure_state_session;
