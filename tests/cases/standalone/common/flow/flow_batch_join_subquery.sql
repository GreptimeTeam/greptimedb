CREATE DATABASE flow_join_fixture;

CREATE TABLE flow_join_fixture."left_samples" (
    source_id STRING,
    left_value DOUBLE,
    event_ts TIMESTAMP,
    observed_at TIMESTAMP TIME INDEX
);

CREATE TABLE flow_join_fixture."right_samples" (
    source_id STRING,
    right_value DOUBLE,
    sample_kind STRING,
    event_ts TIMESTAMP,
    observed_at TIMESTAMP TIME INDEX
);

-- Verify batching flow creation accepts aggregate subqueries joined by LEFT JOIN.
CREATE FLOW flow_batch_join_subquery SINK TO flow_batch_join_sink
EVAL INTERVAL '5m' AS
SELECT
    l.source_id,
    l.measure_name,
    l.bucket_time,
    l.left_event_ts,
    l.left_value,
    r.right_event_ts,
    r.right_value
FROM (
    SELECT
        source_id,
        'sample' AS measure_name,
        date_trunc('minute', now()) AS bucket_time,
        max(event_ts) AS left_event_ts,
        last_value(left_value ORDER BY observed_at) AS left_value
    FROM
        flow_join_fixture."left_samples"
    WHERE
        observed_at BETWEEN date_trunc('minute', now()) - INTERVAL '5 minutes'
            AND date_trunc('minute', now())
    GROUP BY
        source_id
) l
LEFT JOIN (
    SELECT
        source_id,
        'sample' AS measure_name,
        date_trunc('minute', now()) AS bucket_time,
        max(event_ts) AS right_event_ts,
        last_value(right_value ORDER BY observed_at) AS right_value
    FROM
        flow_join_fixture."right_samples"
    WHERE
        observed_at BETWEEN date_trunc('minute', now()) - INTERVAL '5 minutes'
            AND date_trunc('minute', now())
        AND sample_kind = 'primary'
    GROUP BY
        source_id
) r ON l.source_id = r.source_id AND l.bucket_time = r.bucket_time;

SELECT
    source_table_names LIKE '%left_samples%' AS has_left_source,
    source_table_names LIKE '%right_samples%' AS has_right_source,
    options LIKE '%"flow_type":"batching"%' AS is_batching_flow
FROM
    INFORMATION_SCHEMA.FLOWS
WHERE
    flow_name = 'flow_batch_join_subquery';

INSERT INTO flow_join_fixture."left_samples" VALUES
    ('source-a', 0.12, date_trunc('minute', now()), date_trunc('minute', now()));

INSERT INTO flow_join_fixture."right_samples" VALUES
    ('source-a', 100.5, 'primary', date_trunc('minute', now()), date_trunc('minute', now()));

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('flow_batch_join_subquery');

SELECT source_id, measure_name, left_value, right_value FROM flow_batch_join_sink ORDER BY source_id;

DROP FLOW flow_batch_join_subquery;
DROP TABLE flow_batch_join_sink;
DROP TABLE flow_join_fixture."left_samples";
DROP TABLE flow_join_fixture."right_samples";
DROP DATABASE flow_join_fixture;
