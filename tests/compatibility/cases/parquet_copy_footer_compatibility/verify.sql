CREATE TABLE parquet_copy_footer_target(
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val INT
);

COPY parquet_copy_footer_target FROM '${SQLNESS_HOME}/parquet_copy_footer_compatibility/source.parquet' WITH (format='parquet');

SELECT ts, host, val FROM parquet_copy_footer_target ORDER BY ts, host;
