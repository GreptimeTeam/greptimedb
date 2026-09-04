CREATE TABLE application_logs (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2
) WITH (
    'append_mode' = 'true'
);

ALTER TABLE application_logs
    MODIFY COLUMN attrs JSON2 (
        max_auto_expanded_paths = 2000,
        trace_id STRING,
        user.id STRING NOT NULL,
        user.name STRING DEFAULT 'anonymous',
        request_id STRING INVERTED INDEX
    );

DROP TABLE application_logs;
