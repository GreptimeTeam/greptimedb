CREATE TABLE application_logs (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO application_logs VALUES
    (1, '{"service":"frontend","duration":12,"trace_id":"before-1"}'),
    (2, '{"service":"ingester","duration":34,"trace_id":"before-2"}');

ALTER TABLE application_logs
    MODIFY COLUMN attrs JSON2 (
        max_auto_expanded_paths = 2000,
        service STRING,
        duration INT64 DEFAULT 0,
        trace_id STRING
    );

SELECT COUNT(*) AS alter_flushed_sst_num
FROM information_schema.ssts_manifest
WHERE visible
  AND table_id IN (
      SELECT table_id FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = 'application_logs'
  );

INSERT INTO application_logs VALUES
    (3, '{"service":"frontend","duration":56,"trace_id":"after-1"}'),
    (4, '{"service":"compactor","trace_id":"after-2"}');

SELECT ts, attrs.service, attrs.duration, attrs.trace_id
FROM application_logs
ORDER BY ts;

ADMIN FLUSH_TABLE('application_logs');

SELECT ts, attrs.service, attrs.duration, attrs.trace_id
FROM application_logs
ORDER BY ts;

ALTER TABLE application_logs
    MODIFY COLUMN attrs JSON2 (
        trace_id STRING,
        user.id STRING NOT NULL,
        user.name STRING DEFAULT 'anonymous',
        request_id STRING INVERTED INDEX
    );

INSERT INTO application_logs VALUES
    (5, '{"trace_id":"after-3","user":{"id":"u1","name":"alice"},"request_id":"r1"}');

SELECT ts, attrs.trace_id, attrs.user.id, attrs.user.name, attrs.request_id
FROM application_logs
ORDER BY ts;

ALTER TABLE application_logs
    MODIFY COLUMN attrs JSON2 (
        max_auto_expanded_paths = 2000,
        trace_id STRING,
        user.id STRING NOT NULL,
        user.name STRING DEFAULT 'anonymous',
        request_id STRING INVERTED INDEX
    );

INSERT INTO application_logs VALUES
    (6, '{"trace_id":"after-4","user":{"id":"u2"},"request_id":"r2"}');

SELECT ts, attrs.trace_id, attrs.user.id, attrs.user.name, attrs.request_id
FROM application_logs
ORDER BY ts;

ADMIN FLUSH_TABLE('application_logs');

DROP TABLE application_logs;
