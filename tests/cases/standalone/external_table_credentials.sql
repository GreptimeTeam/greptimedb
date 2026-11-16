CREATE TABLE external_table_credentials_source (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE
);

INSERT INTO external_table_credentials_source VALUES (1, 1.0);

COPY external_table_credentials_source TO 'external_table_credentials/table.parquet';

CREATE EXTERNAL TABLE external_table_credentials
WITH (
    location = 'external_table_credentials/table.parquet',
    format = 'parquet',
    access_key_id = 'AKID_PLAIN',
    secret_access_key = 'SECRET_PLAIN',
    session_token = 'TOKEN_PLAIN',
    access_key_secret = 'OSS_SECRET_PLAIN'
);

SHOW CREATE TABLE external_table_credentials;

-- SQLNESS REPLACE __private\.file_table_meta=\S+ FILE_TABLE_META
SELECT create_options FROM information_schema.tables WHERE table_name = 'external_table_credentials';

DROP TABLE external_table_credentials;

DROP TABLE external_table_credentials_source;
