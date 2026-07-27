CREATE TABLE local_file_access_source (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE
);

INSERT INTO local_file_access_source VALUES
    (1, 'host-1', 1.0),
    (2, 'host-2', 2.0);

COPY local_file_access_source TO 'local_file_access/table.parquet';

CREATE EXTERNAL TABLE local_file_access_external
WITH (
    location = 'local_file_access/table.parquet',
    format = 'parquet'
);

SELECT COUNT(*) FROM local_file_access_external;

COPY (SELECT * FROM local_file_access_source)
TO 'local_file_access/query.parquet';

COPY DATABASE public TO 'local_file_access/database/';

COPY local_file_access_source FROM '../escape.parquet';

DROP TABLE local_file_access_external;
DROP TABLE local_file_access_source;
