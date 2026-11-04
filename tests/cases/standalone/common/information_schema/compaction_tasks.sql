DESC TABLE information_schema.compaction_tasks;

SELECT * FROM information_schema.compaction_tasks;

-- These scans require no source columns and must preserve the empty row count.
SELECT 1 FROM information_schema.compaction_tasks;

SELECT count(*) FROM information_schema.compaction_tasks;
