COPY snapshot_values FROM '${SQLNESS_HOME}/snapshot_parquet_copy/values.parquet' WITH (FORMAT='parquet');
SELECT * FROM snapshot_values ORDER BY ts;
SELECT val, host IS NULL AS host_is_null, host = '' AS host_is_empty FROM snapshot_values ORDER BY ts;
COPY snapshot_empty FROM '${SQLNESS_HOME}/snapshot_parquet_copy/empty.parquet' WITH (FORMAT='parquet');
SELECT COUNT(*) FROM snapshot_empty;
