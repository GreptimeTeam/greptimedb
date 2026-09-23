CREATE TABLE session_skip_wal_mysql(host STRING, ts TIMESTAMP TIME INDEX);

CREATE TABLE session_skip_wal_pg(host STRING, ts TIMESTAMP TIME INDEX);

-- This row is written to WAL and then truncated. It must not be replayed after the restart.
INSERT INTO session_skip_wal_pg VALUES ('truncated', 500);

TRUNCATE TABLE session_skip_wal_pg;

-- MYSQL: SET persists across statements on the same connection.
-- SQLNESS PROTOCOL MYSQL
SET skip_wal = true;

-- Invalid SET must leave the enabled policy unchanged.

-- SQLNESS PROTOCOL MYSQL
SET skip_wal = 'invalid';

-- SQLNESS PROTOCOL MYSQL
SET skip_wal = 1;

-- SQLNESS PROTOCOL MYSQL
SET skip_wal = NULL;

-- SQLNESS PROTOCOL MYSQL
SET skip_wal = false, true;

-- SQLNESS PROTOCOL MYSQL
INSERT INTO session_skip_wal_mysql VALUES ('skipped', 1000);

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM session_skip_wal_mysql ORDER BY ts;

-- SQLNESS PROTOCOL MYSQL
SET skip_wal = false;

-- SQLNESS PROTOCOL MYSQL
SET skip_wal = 'true';

-- SQLNESS PROTOCOL MYSQL
SET skip_wal = 'false';

-- SQLNESS PROTOCOL MYSQL
INSERT INTO session_skip_wal_mysql VALUES ('persisted', 2000);

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM session_skip_wal_mysql ORDER BY ts;

-- POSTGRES: SET persists across statements on the same connection.
-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = true;

-- Invalid SET must leave the enabled policy unchanged.

-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = 'invalid';

-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = 1;

-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = NULL;

-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = false, true;

-- SQLNESS PROTOCOL POSTGRES
INSERT INTO session_skip_wal_pg VALUES ('skipped', 1000);

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM session_skip_wal_pg ORDER BY ts;

-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = false;

-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = 'true';

-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = 'false';

-- SQLNESS PROTOCOL POSTGRES
INSERT INTO session_skip_wal_pg VALUES ('persisted', 2000);

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM session_skip_wal_pg ORDER BY ts;

-- Only the 'persisted' rows survive the restart.
-- SQLNESS ARG restart=true
-- SQLNESS PROTOCOL MYSQL
SELECT * FROM session_skip_wal_mysql ORDER BY ts;

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM session_skip_wal_pg ORDER BY ts;

TRUNCATE TABLE session_skip_wal_mysql;

TRUNCATE TABLE session_skip_wal_pg;

DROP TABLE session_skip_wal_mysql;

DROP TABLE session_skip_wal_pg;

-- The default gRPC protocol has request-scoped contexts, not a persistent SQL session.
SET skip_wal = true;

SET skip_wal = false;

SET skip_wal = 'true';

SET skip_wal = 'false';

SET skip_wal = 'invalid';

SET skip_wal = 1;

SET skip_wal = NULL;

SET skip_wal = false, true;
