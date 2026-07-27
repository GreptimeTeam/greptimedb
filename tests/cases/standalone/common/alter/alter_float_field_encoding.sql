CREATE TABLE alter_float_field_encoding (
  ts TIMESTAMP TIME INDEX,
  host STRING PRIMARY KEY,
  val DOUBLE
);

SHOW CREATE TABLE alter_float_field_encoding;

INSERT INTO alter_float_field_encoding VALUES
  (0, 'host-a', 1.25),
  (1, 'host-b', 2.5);

ALTER TABLE alter_float_field_encoding SET 'experimental_sst_float_field_encoding' = 'byte_stream_split';

SHOW CREATE TABLE alter_float_field_encoding;

ADMIN flush_table('alter_float_field_encoding');

SELECT * FROM alter_float_field_encoding ORDER BY ts;

INSERT INTO alter_float_field_encoding VALUES
  (2, 'host-a', 3.75),
  (3, 'host-b', 5.0);

ALTER TABLE alter_float_field_encoding SET 'experimental_sst_float_field_encoding' = 'default';

SHOW CREATE TABLE alter_float_field_encoding;

ADMIN flush_table('alter_float_field_encoding');

SELECT * FROM alter_float_field_encoding ORDER BY ts;

ALTER TABLE alter_float_field_encoding SET 'experimental_sst_float_field_encoding' = 'Byte_Stream_Split';

ALTER TABLE alter_float_field_encoding SET 'experimental_sst_float_field_encoding' = 'unsupported';

SHOW CREATE TABLE alter_float_field_encoding;

DROP TABLE alter_float_field_encoding;

CREATE TABLE alter_float_field_encoding_phy (
  ts TIMESTAMP TIME INDEX,
  val DOUBLE
) ENGINE = metric WITH ("physical_metric_table" = "");

SHOW CREATE TABLE alter_float_field_encoding_phy;

CREATE TABLE alter_float_field_encoding_logical (
  ts TIMESTAMP TIME INDEX,
  val DOUBLE,
  host STRING PRIMARY KEY
) ENGINE = metric WITH ("on_physical_table" = "alter_float_field_encoding_phy");

INSERT INTO alter_float_field_encoding_logical (ts, val, host) VALUES
  ('2022-01-01 00:00:00', 1.25, 'host-a'),
  ('2022-01-01 00:00:01', 2.5, 'host-b');

ALTER TABLE alter_float_field_encoding_phy SET 'experimental_sst_float_field_encoding' = 'byte_stream_split';

SHOW CREATE TABLE alter_float_field_encoding_phy;

ALTER TABLE alter_float_field_encoding_logical SET 'experimental_sst_float_field_encoding' = 'byte_stream_split';

SHOW CREATE TABLE alter_float_field_encoding_phy;

SELECT * FROM alter_float_field_encoding_logical ORDER BY ts;

ALTER TABLE alter_float_field_encoding_phy SET 'experimental_sst_float_field_encoding' = 'default';

SHOW CREATE TABLE alter_float_field_encoding_phy;

ALTER TABLE alter_float_field_encoding_logical SET 'experimental_sst_float_field_encoding' = 'default';

SHOW CREATE TABLE alter_float_field_encoding_phy;

INSERT INTO alter_float_field_encoding_logical (ts, val, host) VALUES
  ('2022-01-01 00:00:02', 3.75, 'host-a'),
  ('2022-01-01 00:00:03', 5.0, 'host-b');

SELECT * FROM alter_float_field_encoding_logical ORDER BY ts;

DROP TABLE alter_float_field_encoding_logical;

DROP TABLE alter_float_field_encoding_phy;
