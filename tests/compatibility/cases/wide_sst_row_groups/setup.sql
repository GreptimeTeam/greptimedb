CREATE TABLE wide_sst_rows (ts TIMESTAMP TIME INDEX, host STRING PRIMARY KEY, payload STRING);
INSERT INTO wide_sst_rows VALUES (1, 'a', repeat('x', 65536)), (2, 'b', repeat('y', 65536));
ADMIN FLUSH_TABLE('wide_sst_rows');
