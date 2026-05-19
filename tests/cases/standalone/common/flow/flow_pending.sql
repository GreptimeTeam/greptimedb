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

DROP FLOW pending_with_defer;

SELECT flow_name FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name = 'pending_with_defer';
