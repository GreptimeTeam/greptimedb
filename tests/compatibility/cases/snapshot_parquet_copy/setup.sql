CREATE TABLE snapshot_values (ts TIMESTAMP TIME INDEX, val BIGINT);
INSERT INTO snapshot_values VALUES (1, 42), (2, 7);
COPY snapshot_values TO '${SQLNESS_HOME}/snapshot_parquet_copy/values.parquet' WITH (FORMAT='parquet');
TRUNCATE TABLE snapshot_values;
