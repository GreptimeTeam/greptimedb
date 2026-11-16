CREATE DATABASE drop_database_logical_tables;
USE drop_database_logical_tables;

CREATE TABLE physical (ts TIMESTAMP TIME INDEX, val DOUBLE)
ENGINE=metric WITH ("physical_metric_table" = "");
CREATE TABLE logical_a (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY)
ENGINE=metric WITH ("on_physical_table" = "physical");
CREATE TABLE logical_b (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY)
ENGINE=metric WITH ("on_physical_table" = "physical");
CREATE TABLE ordinary (ts TIMESTAMP TIME INDEX, val DOUBLE);
CREATE VIEW logical_view AS SELECT * FROM logical_a;

INSERT INTO logical_a (ts, val, host) VALUES (0, 1, 'a');
INSERT INTO logical_b (ts, val, host) VALUES (0, 2, 'b');
SELECT * FROM logical_view;
SHOW TABLES;

USE public;
DROP DATABASE drop_database_logical_tables;
SHOW TABLES FROM drop_database_logical_tables;

-- Reusing names must not retain the dropped tables or their rows.
CREATE DATABASE drop_database_logical_tables;
USE drop_database_logical_tables;
CREATE TABLE physical (ts TIMESTAMP TIME INDEX, val DOUBLE)
ENGINE=metric WITH ("physical_metric_table" = "");
CREATE TABLE logical_a (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY)
ENGINE=metric WITH ("on_physical_table" = "physical");
SELECT COUNT(*) FROM logical_a;
INSERT INTO logical_a (ts, val, host) VALUES (0, 7, 'new');

-- SQLNESS ARG restart=true
SELECT * FROM logical_a;

USE public;
DROP DATABASE drop_database_logical_tables;
DROP DATABASE IF EXISTS drop_database_logical_tables;
