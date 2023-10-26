CREATE TABLE data_types (
  s string,
  tint int8,
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
