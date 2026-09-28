COPY snapshot_values FROM '${SQLNESS_HOME}/snapshot_parquet_copy/values.parquet' WITH (FORMAT='parquet');
SELECT * FROM snapshot_values ORDER BY ts;
