CREATE TABLE mito_float_field_encoding (ts TIMESTAMP TIME INDEX, host STRING PRIMARY KEY, val DOUBLE)
ENGINE=mito WITH ('experimental_sst_float_field_encoding' = 'default');

INSERT INTO mito_float_field_encoding VALUES ('2024-01-01 00:00:00', 'host_a', 1.25);
ADMIN FLUSH_TABLE('mito_float_field_encoding');
ALTER TABLE mito_float_field_encoding SET 'experimental_sst_float_field_encoding' = 'byte_stream_split';
INSERT INTO mito_float_field_encoding VALUES ('2024-01-01 00:01:00', 'host_b', 2.5);
ADMIN FLUSH_TABLE('mito_float_field_encoding');
SELECT sum(val) FROM mito_float_field_encoding;
SELECT ts, host, val FROM mito_float_field_encoding ORDER BY ts;
SHOW CREATE TABLE mito_float_field_encoding;
ALTER TABLE mito_float_field_encoding SET 'experimental_sst_float_field_encoding' = 'invalid';
ALTER TABLE mito_float_field_encoding UNSET 'experimental_sst_float_field_encoding';
SHOW CREATE TABLE mito_float_field_encoding;

CREATE TABLE metric_float_field_encoding_physical (ts TIMESTAMP TIME INDEX, val DOUBLE)
ENGINE=metric WITH ('physical_metric_table' = '', 'experimental_sst_float_field_encoding' = 'default');
CREATE TABLE metric_float_field_encoding (ts TIMESTAMP TIME INDEX, host STRING PRIMARY KEY, val DOUBLE)
ENGINE=metric WITH ('on_physical_table' = 'metric_float_field_encoding_physical');
INSERT INTO metric_float_field_encoding (ts, host, val) VALUES ('2024-01-01 00:00:00', 'host_a', 1.25);
ADMIN FLUSH_TABLE('metric_float_field_encoding_physical');
ALTER TABLE metric_float_field_encoding_physical SET 'experimental_sst_float_field_encoding' = 'byte_stream_split';
INSERT INTO metric_float_field_encoding (ts, host, val) VALUES ('2024-01-01 00:01:00', 'host_b', 2.5);
ADMIN FLUSH_TABLE('metric_float_field_encoding_physical');
SELECT sum(val) FROM metric_float_field_encoding;
SELECT ts, host, val FROM metric_float_field_encoding ORDER BY ts;
SHOW CREATE TABLE metric_float_field_encoding_physical;
ALTER TABLE metric_float_field_encoding SET 'experimental_sst_float_field_encoding' = 'byte_stream_split';
ALTER TABLE metric_float_field_encoding_physical UNSET 'experimental_sst_float_field_encoding';
SHOW CREATE TABLE metric_float_field_encoding_physical;

DROP TABLE metric_float_field_encoding;
DROP TABLE metric_float_field_encoding_physical;
DROP TABLE mito_float_field_encoding;
