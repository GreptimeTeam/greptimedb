CREATE TABLE legacy_columns (ts TIMESTAMP TIME INDEX, payload STRING, tokens BIGINT);
INSERT INTO legacy_columns VALUES (1, 'before upgrade', 100), (2, 'second row', 200);
ADMIN FLUSH_TABLE('legacy_columns');
