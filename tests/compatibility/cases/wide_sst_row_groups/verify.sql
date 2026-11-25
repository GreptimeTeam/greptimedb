SELECT ts, host, length(payload) FROM wide_sst_rows ORDER BY ts;
INSERT INTO wide_sst_rows VALUES (3, 'c', repeat('z', 65536));
ADMIN FLUSH_TABLE('wide_sst_rows');
ADMIN COMPACT_TABLE('wide_sst_rows', 'strict_window', 'window=3600');
SELECT ts, host, length(payload) FROM wide_sst_rows ORDER BY ts;
DROP TABLE wide_sst_rows;
