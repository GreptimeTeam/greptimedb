DESC TABLE information_schema.compaction_tasks;

SELECT * FROM information_schema.compaction_tasks WHERE node_id = 18446744073709551615;

-- Constant projections and aggregates must preserve the empty row count.
SELECT 1 FROM information_schema.compaction_tasks WHERE node_id = 18446744073709551615;

SELECT count(*) FROM information_schema.compaction_tasks WHERE node_id = 18446744073709551615;
