CREATE TABLE snapshot_values (ts TIMESTAMP TIME INDEX, val BIGINT, host STRING);
INSERT INTO snapshot_values VALUES (1, 42, ''), (2, 7, NULL);
COPY snapshot_values TO '${SQLNESS_HOME}/snapshot_parquet_copy/values.parquet' WITH (FORMAT='parquet');
TRUNCATE TABLE snapshot_values;
CREATE TABLE snapshot_empty (ts TIMESTAMP TIME INDEX, val DOUBLE);
COPY snapshot_empty TO '${SQLNESS_HOME}/snapshot_parquet_copy/empty.parquet' WITH (FORMAT='parquet');
