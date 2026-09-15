SELECT ts, host, f, d
FROM t_sst_float_field_encoding_upgrade
ORDER BY ts, host;

ALTER TABLE t_sst_float_field_encoding_upgrade SET 'experimental_sst_float_field_encoding' = 'byte_stream_split';

INSERT INTO t_sst_float_field_encoding_upgrade VALUES
('2024-02-10 00:03:00+0000', 'host_d', 3.75, 30.5);

ADMIN FLUSH_TABLE('t_sst_float_field_encoding_upgrade');

SELECT ts, host, f, d
FROM t_sst_float_field_encoding_upgrade
ORDER BY ts, host;

SHOW CREATE TABLE t_sst_float_field_encoding_upgrade;

-- SQLNESS ARG restart=true
SHOW CREATE TABLE t_sst_float_field_encoding_upgrade;

INSERT INTO t_sst_float_field_encoding_upgrade VALUES
('2024-02-10 00:04:00+0000', 'host_e', 4.5, 40.25);

ADMIN FLUSH_TABLE('t_sst_float_field_encoding_upgrade');

SELECT ts, host, f, d
FROM t_sst_float_field_encoding_upgrade
ORDER BY ts, host;

ALTER TABLE t_sst_float_field_encoding_upgrade UNSET 'experimental_sst_float_field_encoding';

SHOW CREATE TABLE t_sst_float_field_encoding_upgrade;
