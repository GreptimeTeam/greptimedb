-- The streaming runtime is gone, so the batching scheduler is the only execution
-- profile left. A flow whose SQL has no time window is exactly the shape the
-- streaming executor used to handle: it is now rejected, and the supported
-- migration is to recreate the flow with an explicit `EVAL INTERVAL`.
CREATE TABLE legacy_flow_src (
    host STRING,
    val DOUBLE,
    ts TIMESTAMP TIME INDEX
);

CREATE TABLE legacy_flow_sink (
    host STRING,
    val DOUBLE,
    ts TIMESTAMP TIME INDEX
);

CREATE FLOW legacy_flow SINK TO legacy_flow_sink AS
SELECT host, val, ts FROM legacy_flow_src;

SELECT count(*) FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name = 'legacy_flow';

-- Recreating the flow with a schedule is the supported migration.
CREATE FLOW legacy_flow SINK TO legacy_flow_sink EVAL INTERVAL '1h' AS
SELECT host, val, ts FROM legacy_flow_src;

-- The persisted option is still `batching`, so the metadata shape is unchanged.
SELECT flow_name, options FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name = 'legacy_flow';

INSERT INTO legacy_flow_src VALUES ('host_a', 1.0, '2024-01-01 00:00:00');

-- Mirror inserts reach the flownode asynchronously; wait before flushing.
-- SQLNESS SLEEP 3s
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('legacy_flow');

SELECT host, val, ts FROM legacy_flow_sink ORDER BY ts;

DROP FLOW legacy_flow;

SELECT count(*) FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name = 'legacy_flow';

DROP TABLE legacy_flow_src;

DROP TABLE legacy_flow_sink;
