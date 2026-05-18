CREATE FLOW pending_without_defer
SINK TO pending_sink
AS SELECT val FROM pending_source;

CREATE FLOW pending_with_defer
SINK TO pending_sink
WITH (defer_on_missing_source = true)
AS SELECT val FROM pending_source WHERE val > 10;

SHOW CREATE FLOW pending_with_defer;

SELECT
    flow_definition,
    source_table_ids,
    source_table_names,
    flownode_ids,
    options LIKE '%"defer_on_missing_source":"true"%' AS has_defer_option,
    options LIKE '%"flow_type":"batching"%' AS has_flow_type_option
FROM INFORMATION_SCHEMA.FLOWS
WHERE flow_name = 'pending_with_defer';

CREATE TABLE pending_source (
    val INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    TIME INDEX(ts)
);

-- SQLNESS SLEEP 12s

SELECT
    flow_definition,
    source_table_ids,
    source_table_names,
    flownode_ids,
    options LIKE '%"defer_on_missing_source":"true"%' AS has_defer_option,
    options LIKE '%"flow_type":"batching"%' AS has_flow_type_option
FROM INFORMATION_SCHEMA.FLOWS
WHERE flow_name = 'pending_with_defer';

INSERT INTO pending_source VALUES (10, 0), (11, 1), (12, 2);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('pending_with_defer');

SELECT val FROM pending_sink ORDER BY val;

CREATE OR REPLACE FLOW pending_with_defer
SINK TO pending_sink
WITH (defer_on_missing_source = true)
AS SELECT val FROM pending_replacement_source WHERE val > 100;

SELECT
    flow_definition,
    source_table_ids,
    source_table_names,
    flownode_ids,
    options LIKE '%"defer_on_missing_source":"true"%' AS has_defer_option,
    options LIKE '%"flow_type":"batching"%' AS has_flow_type_option
FROM INFORMATION_SCHEMA.FLOWS
WHERE flow_name = 'pending_with_defer';

CREATE TABLE pending_replacement_source (
    val INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    TIME INDEX(ts)
);

-- SQLNESS SLEEP 12s

INSERT INTO pending_replacement_source VALUES (99, 3), (101, 4), (102, 5);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('pending_with_defer');

SELECT val FROM pending_sink ORDER BY val;

DROP FLOW pending_with_defer;

DROP TABLE pending_sink;

DROP TABLE pending_source;

DROP TABLE pending_replacement_source;
