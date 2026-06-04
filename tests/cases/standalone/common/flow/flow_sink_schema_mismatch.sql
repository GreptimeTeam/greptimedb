-- Verify that batching flow rejects CREATE FLOW when the pre-existing sink
-- table schema does not match the flow output (create-time validation, not runtime).
CREATE TABLE source_mm (
    "number" INT,
    extra STRING,
    ts TIMESTAMP TIME INDEX
);

-- Pre-create a sink table that is intentionally missing the "extra" column.
-- This case validates batching mode at CREATE FLOW time, before any INSERT/FLUSH.
CREATE TABLE sink_mm (
    "number" INT,
    time_window TIMESTAMP TIME INDEX,
    cnt BIGINT
);

-- This CREATE FLOW should fail immediately: the flow outputs (number, extra, time_window, cnt)
-- but sink_mm has only (number, time_window, cnt).
-- SQLNESS REPLACE (in\scontext:\sFailed\sto\srewrite\splan:\sError\sduring\splanning:.*) in context: Failed to rewrite plan
CREATE FLOW mismatch_flow SINK TO sink_mm AS
SELECT
    "number",
    extra,
    date_bin(INTERVAL '1 second', ts) as time_window,
    count(*) as cnt
FROM
    source_mm
GROUP BY
    "number", extra, time_window;

DROP TABLE source_mm;
DROP TABLE sink_mm;

-- TQL/PromQL flows use the same create-time sink schema validation path.
CREATE TABLE tql_source_mm (
    `value` DOUBLE,
    ts TIMESTAMP TIME INDEX,
    sensor STRING,
    loc STRING,
    PRIMARY KEY (sensor, loc)
);

-- Pre-create a TQL sink table that is intentionally missing the "sensor" tag column.
CREATE TABLE tql_sink_mm (
    `value` DOUBLE,
    ts TIMESTAMP TIME INDEX
);

-- This CREATE FLOW should fail immediately: the TQL output has (value, sensor, ts),
-- but tql_sink_mm has only (value, ts).
-- SQLNESS REPLACE (in\scontext:\sFailed\sto\srewrite\splan:\sError\sduring\splanning:.*) in context: Failed to rewrite plan
CREATE FLOW tql_mismatch_flow
SINK TO tql_sink_mm
EVAL INTERVAL '1m' AS
TQL EVAL (now() - '1m'::interval, now(), '1m')
avg by(sensor) (tql_source_mm) AS value;

DROP TABLE tql_source_mm;
DROP TABLE tql_sink_mm;

-- Real merge_mode=last_non_null sink options should enable partial schema validation.
CREATE TABLE lnn_source_mm (
    device STRING,
    val DOUBLE,
    ts TIMESTAMP TIME INDEX
);

CREATE TABLE lnn_sink_mm (
    device STRING,
    time_window TIMESTAMP TIME INDEX,
    cnt BIGINT,
    PRIMARY KEY (device)
) WITH('merge_mode'='last_non_null');

-- This CREATE FLOW should fail through the last_non_null partial validator: the
-- sink primary key "device" is required but absent from the flow output.
-- SQLNESS REPLACE (in\scontext:\sFailed\sto\srewrite\splan:\sError\sduring\splanning:.*) in context: Failed to rewrite plan
CREATE FLOW lnn_missing_pk_flow
SINK TO lnn_sink_mm AS
SELECT
    date_bin(INTERVAL '1 second', ts) as time_window,
    count(*) as cnt
FROM
    lnn_source_mm
GROUP BY
    time_window;

DROP TABLE lnn_source_mm;
DROP TABLE lnn_sink_mm;
