SELECT * FROM compaction_parallelism ORDER BY ts;

ALTER TABLE compaction_parallelism SET 'compaction.experimental_output_parallelism' = '4';
INSERT INTO compaction_parallelism VALUES (2000, 30);
ADMIN FLUSH_TABLE('compaction_parallelism');
ADMIN COMPACT_TABLE('compaction_parallelism');

-- SQLNESS ARG restart=true
SHOW CREATE TABLE compaction_parallelism;
SELECT * FROM compaction_parallelism ORDER BY ts;

ALTER TABLE compaction_parallelism UNSET 'compaction.experimental_output_parallelism';
-- SQLNESS ARG restart=true
SHOW CREATE TABLE compaction_parallelism;
SELECT * FROM compaction_parallelism ORDER BY ts;
