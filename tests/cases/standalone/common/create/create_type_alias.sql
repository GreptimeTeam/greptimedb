CREATE TABLE data_types (
  s string,
  i2 int2,
  i4 int4,
  i8 int8,
  f4 float4,
  f8 float8,
  u64 uint64,
  u32 uint32,
  u16 uint16,
  u8 uint8,
  sint int16,
  i INT32,
  bint INT64,
  v varchar,
  f FLOAT32,
  d FLOAT64,
  b boolean,
  vb varbinary,
  dt date,
  dtt datetime,
  ts0 TimestampSecond,
  ts3 Timestamp_MS,
  ts6 Timestamp_US,
  ts9 TimestampNanosecond DEFAULT CURRENT_TIMESTAMP() TIME INDEX,
  PRIMARY KEY(s));

SHOW CREATE TABLE data_types;

DESC TABLE data_types;

DROP TABLE data_types;
