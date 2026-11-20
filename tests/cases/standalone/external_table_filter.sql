COPY (
    SELECT * FROM (VALUES
        (1, 'a', '2024-01-01T00:00:00.000'),
        (2, 'b', '2024-01-02T00:00:00.000'),
        (3, 'c', '2024-01-03T00:00:00.000')
    ) AS rows(id, host, ts) ORDER BY id
) TO 'external_table_filter/rows.csv' WITH (format = 'csv');

CREATE EXTERNAL TABLE csv_filter_declared (
    "id" BIGINT,
    "host" STRING PRIMARY KEY,
    "ts" TIMESTAMP(3) TIME INDEX
) WITH (location = 'external_table_filter/rows.csv', format = 'csv');

SELECT id FROM csv_filter_declared WHERE host = 'b';

SELECT id FROM csv_filter_declared WHERE ts >= TIMESTAMP '2024-01-03 00:00:00';

SELECT id FROM csv_filter_declared WHERE host = 'b' LIMIT 1;

CREATE EXTERNAL TABLE csv_filter_inferred
WITH (location = 'external_table_filter/rows.csv', format = 'csv');

SELECT COUNT(*) FROM csv_filter_inferred
WHERE greptime_timestamp > TIMESTAMP '2000-01-01 00:00:00';

DROP TABLE csv_filter_inferred;

COPY (SELECT * FROM csv_filter_declared ORDER BY id)
TO 'external_table_filter/rows.parquet' WITH (format = 'parquet');

CREATE EXTERNAL TABLE parquet_filter_declared (
    "id" BIGINT,
    "host" STRING PRIMARY KEY,
    "ts" TIMESTAMP(3) TIME INDEX
) WITH (location = 'external_table_filter/rows.parquet', format = 'parquet');

-- The matching row shares a row group with two nonmatching rows.
SELECT id FROM parquet_filter_declared WHERE host = 'b';

DROP TABLE parquet_filter_declared;

DROP TABLE csv_filter_declared;
