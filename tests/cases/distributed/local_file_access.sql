CREATE TABLE local_file_access_distributed (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE
);

INSERT INTO local_file_access_distributed VALUES (1, 'host-1', 1.0);

COPY local_file_access_distributed
TO 'local_file_access/table.parquet';

COPY local_file_access_distributed
FROM 'local_file_access/table.parquet';

COPY (SELECT * FROM local_file_access_distributed)
TO 'local_file_access/query.parquet';

COPY DATABASE public
TO 'local_file_access/database/';

COPY DATABASE public
FROM 'local_file_access/database/';

CREATE EXTERNAL TABLE local_file_access_external
WITH (
    location = 'local_file_access/table.parquet',
    format = 'parquet'
);

DROP TABLE local_file_access_distributed;
