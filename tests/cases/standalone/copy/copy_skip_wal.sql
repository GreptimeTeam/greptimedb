CREATE TABLE copy_skip_wal(host STRING, ts TIMESTAMP TIME INDEX);

INSERT INTO copy_skip_wal VALUES ('host1', 1000), ('host2', 2000);

COPY copy_skip_wal TO '${SQLNESS_HOME}/copy_skip_wal/copy_skip_wal.csv' WITH (FORMAT='csv');

TRUNCATE TABLE copy_skip_wal;

-- MYSQL: COPY TABLE inherits the connection WAL policy.
-- SQLNESS PROTOCOL MYSQL
SET skip_wal = true;

-- SQLNESS PROTOCOL MYSQL
COPY copy_skip_wal FROM '${SQLNESS_HOME}/copy_skip_wal/copy_skip_wal.csv' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal ORDER BY ts;

-- SQLNESS ARG restart=true
-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal ORDER BY ts;

-- SQLNESS PROTOCOL MYSQL
SET skip_wal = false;

-- SQLNESS PROTOCOL MYSQL
COPY copy_skip_wal FROM '${SQLNESS_HOME}/copy_skip_wal/copy_skip_wal.csv' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal ORDER BY ts;

-- SQLNESS ARG restart=true
-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal ORDER BY ts;

TRUNCATE TABLE copy_skip_wal;

-- MYSQL: COPY DATABASE inherits the connection WAL policy.
-- SQLNESS PROTOCOL MYSQL
SET skip_wal = true;

-- SQLNESS PROTOCOL MYSQL
COPY DATABASE public FROM '${SQLNESS_HOME}/copy_skip_wal/' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal ORDER BY ts;

-- SQLNESS ARG restart=true
-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal ORDER BY ts;

-- SQLNESS PROTOCOL MYSQL
SET skip_wal = false;

-- SQLNESS PROTOCOL MYSQL
COPY DATABASE public FROM '${SQLNESS_HOME}/copy_skip_wal/' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal ORDER BY ts;

-- SQLNESS ARG restart=true
-- SQLNESS PROTOCOL MYSQL
SELECT * FROM copy_skip_wal ORDER BY ts;

TRUNCATE TABLE copy_skip_wal;

-- POSTGRES: COPY TABLE inherits the connection WAL policy.
-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = true;

-- SQLNESS PROTOCOL POSTGRES
COPY copy_skip_wal FROM '${SQLNESS_HOME}/copy_skip_wal/copy_skip_wal.csv' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal ORDER BY ts;

-- SQLNESS ARG restart=true
-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal ORDER BY ts;

-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = false;

-- SQLNESS PROTOCOL POSTGRES
COPY copy_skip_wal FROM '${SQLNESS_HOME}/copy_skip_wal/copy_skip_wal.csv' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal ORDER BY ts;

-- SQLNESS ARG restart=true
-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal ORDER BY ts;

TRUNCATE TABLE copy_skip_wal;

-- POSTGRES: COPY DATABASE inherits the connection WAL policy.
-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = true;

-- SQLNESS PROTOCOL POSTGRES
COPY DATABASE public FROM '${SQLNESS_HOME}/copy_skip_wal/' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal ORDER BY ts;

-- SQLNESS ARG restart=true
-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal ORDER BY ts;

-- SQLNESS PROTOCOL POSTGRES
SET skip_wal = false;

-- SQLNESS PROTOCOL POSTGRES
COPY DATABASE public FROM '${SQLNESS_HOME}/copy_skip_wal/' WITH (FORMAT='csv');

-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal ORDER BY ts;

-- SQLNESS ARG restart=true
-- SQLNESS PROTOCOL POSTGRES
SELECT * FROM copy_skip_wal ORDER BY ts;

TRUNCATE TABLE copy_skip_wal;

DROP TABLE copy_skip_wal;
