CREATE TABLE parquet_copy_footer_source(
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val INT
);

INSERT INTO parquet_copy_footer_source VALUES
('2024-02-09 00:00:00+0000', 'host_a', 1),
('2024-02-09 00:01:00+0000', 'host_b', 2);

COPY parquet_copy_footer_source TO '${SQLNESS_HOME}/parquet_copy_footer_compatibility/source.parquet' WITH (format='parquet');
