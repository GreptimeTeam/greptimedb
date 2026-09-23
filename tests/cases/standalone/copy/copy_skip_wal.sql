CREATE TABLE copy_skip_wal(host STRING, ts TIMESTAMP TIME INDEX);

INSERT INTO copy_skip_wal VALUES ('host1', 1000), ('host2', 2000);

COPY copy_skip_wal TO '${SQLNESS_HOME}/copy_skip_wal/copy_skip_wal.csv' WITH (FORMAT='csv');

-- COPY DATABASE imports every file in the directory, so each COPY DATABASE case gets its own directory.
COPY copy_skip_wal TO '${SQLNESS_HOME}/copy_skip_wal_mysql_db_skip/copy_skip_wal_mysql_db_skip.csv' WITH (FORMAT='csv');

COPY copy_skip_wal TO '${SQLNESS_HOME}/copy_skip_wal_mysql_db_wal/copy_skip_wal_mysql_db_wal.csv' WITH (FORMAT='csv');

COPY copy_skip_wal TO '${SQLNESS_HOME}/copy_skip_wal_pg_db_skip/copy_skip_wal_pg_db_skip.csv' WITH (FORMAT='csv');

COPY copy_skip_wal TO '${SQLNESS_HOME}/copy_skip_wal_pg_db_wal/copy_skip_wal_pg_db_wal.csv' WITH (FORMAT='csv');

CREATE TABLE copy_skip_wal_mysql_table_skip(host STRING, ts TIMESTAMP TIME INDEX);

CREATE TABLE copy_skip_wal_mysql_table_wal(host STRING, ts TIMESTAMP TIME INDEX);

CREATE TABLE copy_skip_wal_mysql_db_skip(host STRING, ts TIMESTAMP TIME INDEX);

CREATE TABLE copy_skip_wal_mysql_db_wal(host STRING, ts TIMESTAMP TIME INDEX);

CREATE TABLE copy_skip_wal_pg_table_skip(host STRING, ts TIMESTAMP TIME INDEX);

CREATE TABLE copy_skip_wal_pg_table_wal(host STRING, ts TIMESTAMP TIME INDEX);

CREATE TABLE copy_skip_wal_pg_db_skip(host STRING, ts TIMESTAMP TIME INDEX);

CREATE TABLE copy_skip_wal_pg_db_wal(host STRING, ts TIMESTAMP TIME INDEX);

-- Rows written to WAL and then truncated must not be replayed after the restart.
INSERT INTO copy_skip_wal_mysql_table_skip VALUES ('host1', 1000), ('host2', 2000);

TRUNCATE TABLE copy_skip_wal_mysql_table_skip;

INSERT INTO copy_skip_wal_mysql_db_skip VALUES ('host1', 1000), ('host2', 2000);

TRUNCATE TABLE copy_skip_wal_mysql_db_skip;

INSERT INTO copy_skip_wal_pg_table_skip VALUES ('host1', 1000), ('host2', 2000);

TRUNCATE TABLE copy_skip_wal_pg_table_skip;

INSERT INTO copy_skip_wal_pg_db_skip VALUES ('host1', 1000), ('host2', 2000);

TRUNCATE TABLE copy_skip_wal_pg_db_skip;

-- MYSQL: COPY TABLE inherits the connection WAL policy.
-- SQLNESS PROTOCOL MYSQL
SET skip_wal = true;

-- SQLNESS PROTOCOL MYSQL
COPY copy_skip_wal_mysql_table_skip FROM '${SQLNESS_HOME}/copy_skip_wal/copy_skip_wal.csv' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal_mysql_table_skip ORDER BY ts;

-- SQLNESS PROTOCOL MYSQL
SET skip_wal = false;

-- SQLNESS PROTOCOL MYSQL
COPY copy_skip_wal_mysql_table_wal FROM '${SQLNESS_HOME}/copy_skip_wal/copy_skip_wal.csv' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal_mysql_table_wal ORDER BY ts;

-- MYSQL: COPY DATABASE inherits the connection WAL policy.
-- SQLNESS PROTOCOL MYSQL
SET skip_wal = true;

-- SQLNESS PROTOCOL MYSQL
COPY DATABASE public FROM '${SQLNESS_HOME}/copy_skip_wal_mysql_db_skip/' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal_mysql_db_skip ORDER BY ts;

-- SQLNESS PROTOCOL MYSQL
SET skip_wal = false;

-- SQLNESS PROTOCOL MYSQL
COPY DATABASE public FROM '${SQLNESS_HOME}/copy_skip_wal_mysql_db_wal/' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal_mysql_db_wal ORDER BY ts;

-- POSTGRES: COPY TABLE inherits the connection WAL policy.
-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = true;

-- SQLNESS PROTOCOL POSTGRES
COPY copy_skip_wal_pg_table_skip FROM '${SQLNESS_HOME}/copy_skip_wal/copy_skip_wal.csv' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal_pg_table_skip ORDER BY ts;

-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = false;

-- SQLNESS PROTOCOL POSTGRES
COPY copy_skip_wal_pg_table_wal FROM '${SQLNESS_HOME}/copy_skip_wal/copy_skip_wal.csv' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal_pg_table_wal ORDER BY ts;

-- POSTGRES: COPY DATABASE inherits the connection WAL policy.
-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = true;

-- SQLNESS PROTOCOL POSTGRES
COPY DATABASE public FROM '${SQLNESS_HOME}/copy_skip_wal_pg_db_skip/' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal_pg_db_skip ORDER BY ts;

-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = false;

-- SQLNESS PROTOCOL POSTGRES
COPY DATABASE public FROM '${SQLNESS_HOME}/copy_skip_wal_pg_db_wal/' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal_pg_db_wal ORDER BY ts;

-- Only the tables copied with skip_wal = false keep their rows after the restart.
-- SQLNESS ARG restart=true
-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal_mysql_table_skip ORDER BY ts;

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal_mysql_table_wal ORDER BY ts;

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal_mysql_db_skip ORDER BY ts;

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal_mysql_db_wal ORDER BY ts;

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal_pg_table_skip ORDER BY ts;

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal_pg_table_wal ORDER BY ts;

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal_pg_db_skip ORDER BY ts;

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal_pg_db_wal ORDER BY ts;

DROP TABLE copy_skip_wal;

DROP TABLE copy_skip_wal_mysql_table_skip;

DROP TABLE copy_skip_wal_mysql_table_wal;

DROP TABLE copy_skip_wal_mysql_db_skip;

DROP TABLE copy_skip_wal_mysql_db_wal;

DROP TABLE copy_skip_wal_pg_table_skip;

DROP TABLE copy_skip_wal_pg_table_wal;

DROP TABLE copy_skip_wal_pg_db_skip;

DROP TABLE copy_skip_wal_pg_db_wal;
