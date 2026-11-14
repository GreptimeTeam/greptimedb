CREATE TABLE replace_type_input (
    v STRING,
    ts TIMESTAMP TIME INDEX
);

CREATE TABLE replace_type_batch_old (
    v STRING,
    ts TIMESTAMP TIME INDEX
);
CREATE TABLE replace_type_stream (
    v STRING,
    ts TIMESTAMP TIME INDEX
);
CREATE TABLE replace_type_batch_new (
    v STRING,
    ts TIMESTAMP TIME INDEX
);
CREATE TABLE replace_type_bad_sink (
    v STRING,
    ts TIMESTAMP TIME INDEX
);

-- EVAL INTERVAL selects batching mode for this non-aggregate query.
CREATE FLOW replace_type_flow
SINK TO replace_type_batch_old
EVAL INTERVAL '1s'
AS SELECT v, ts FROM replace_type_input;

SELECT options FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name = 'replace_type_flow';

INSERT INTO replace_type_input VALUES ('before_batch_to_stream', '2024-01-01 00:00:00');
-- SQLNESS SLEEP 3s
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('replace_type_flow');

-- Removing EVAL INTERVAL selects streaming mode; use a distinct sink so old
-- batching-engine writes cannot be hidden by primary-key upserts.
CREATE OR REPLACE FLOW replace_type_flow
SINK TO replace_type_stream
AS SELECT v, ts FROM replace_type_input;

SELECT options FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name = 'replace_type_flow';

-- A failed cross-mode replacement must leave the current streaming flow usable.
-- SQLNESS REPLACE (in\scontext:.*) in context: Failed to rewrite plan
CREATE OR REPLACE FLOW replace_type_flow
SINK TO replace_type_bad_sink
EVAL INTERVAL '1s'
AS SELECT v, v AS extra, ts FROM replace_type_input;

SELECT options FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name = 'replace_type_flow';

INSERT INTO replace_type_input VALUES ('after_batch_to_stream', '2024-01-01 00:00:01');
-- SQLNESS SLEEP 3s
SELECT v FROM replace_type_batch_old ORDER BY ts;
SELECT v FROM replace_type_stream ORDER BY ts;

-- Replacing streaming with EVAL INTERVAL selects batching mode again.
CREATE OR REPLACE FLOW replace_type_flow
SINK TO replace_type_batch_new
EVAL INTERVAL '1s'
AS SELECT v, ts FROM replace_type_input;

SELECT options FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name = 'replace_type_flow';

INSERT INTO replace_type_input VALUES ('after_stream_to_batch', '2024-01-01 00:00:02');
-- SQLNESS SLEEP 3s
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('replace_type_flow');
SELECT v FROM replace_type_stream ORDER BY ts;
SELECT v FROM replace_type_batch_new ORDER BY ts;

DROP FLOW replace_type_flow;
INSERT INTO replace_type_input VALUES ('after_drop', '2024-01-01 00:00:03');
-- SQLNESS SLEEP 3s
SELECT v FROM replace_type_batch_old ORDER BY ts;
SELECT v FROM replace_type_stream ORDER BY ts;
SELECT v FROM replace_type_batch_new ORDER BY ts;

DROP TABLE replace_type_bad_sink;
DROP TABLE replace_type_batch_new;
DROP TABLE replace_type_stream;
DROP TABLE replace_type_batch_old;
DROP TABLE replace_type_input;
