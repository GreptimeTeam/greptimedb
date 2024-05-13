--- test CREATE VIEW ---

CREATE DATABASE for_test_view;

USE for_test_view;

CREATE TABLE test_table(a STRING, ts TIMESTAMP TIME INDEX);

CREATE VIEW test_view;

CREATE VIEW test_view as DELETE FROM public.numbers;

--- Table already exists ---
CREATE VIEW test_table as SELECT * FROM public.numbers;

--- Table already exists even when create_if_not_exists ---
CREATE VIEW IF NOT EXISTS test_table as SELECT * FROM public.numbers;

--- Table already exists even when or_replace ---
CREATE OR REPLACE VIEW test_table as SELECT * FROM public.numbers;

CREATE VIEW test_view as SELECT * FROM public.numbers;

--- View already exists ----
CREATE VIEW test_view as SELECT * FROM public.numbers;

CREATE VIEW IF NOT EXISTS test_view as SELECT * FROM public.numbers;

CREATE OR REPLACE VIEW test_view as SELECT * FROM public.numbers;

SHOW TABLES;

SHOW FULL TABLES;

-- SQLNESS REPLACE (\s\d+\s) ID
SELECT * FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME, TABLE_TYPE;

-- SQLNESS REPLACE (\s\d+\s) ID
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'VIEW';

SHOW COLUMNS FROM test_view;

SHOW FULL COLUMNS FROM test_view;

SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'test_view';

--- FIXED in the following PR ---
SELECT * FROM test_view;

USE public;

DROP DATABASE for_test_view;
