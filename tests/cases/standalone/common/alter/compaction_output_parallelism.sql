-- Execution parallelism does not require compaction.type.
CREATE TABLE compaction_output_parallelism (
    ts TIMESTAMP TIME INDEX,
    val INT
) WITH ('compaction.experimental_output_parallelism' = '4');

INSERT INTO compaction_output_parallelism VALUES (0, 10);
ADMIN FLUSH_TABLE('compaction_output_parallelism');
INSERT INTO compaction_output_parallelism VALUES (1000, 20);
ADMIN FLUSH_TABLE('compaction_output_parallelism');
ADMIN COMPACT_TABLE('compaction_output_parallelism');

-- SQLNESS ARG restart=true
SHOW CREATE TABLE compaction_output_parallelism;

SELECT * FROM compaction_output_parallelism ORDER BY ts;

-- A table can disable parallel encoding even when the engine default enables it.
ALTER TABLE compaction_output_parallelism SET 'compaction.experimental_output_parallelism' = '1';
SHOW CREATE TABLE compaction_output_parallelism;

ALTER TABLE compaction_output_parallelism SET 'compaction.experimental_output_parallelism' = '0';
ALTER TABLE compaction_output_parallelism SET 'compaction.experimental_output_parallelism' = '-1';
ALTER TABLE compaction_output_parallelism SET 'compaction.experimental_output_parallelism' = 'invalid';

-- UNSET removes the override without introducing compaction.type or compaction.override.
ALTER TABLE compaction_output_parallelism UNSET 'compaction.experimental_output_parallelism';
-- SQLNESS ARG restart=true
SHOW CREATE TABLE compaction_output_parallelism;

SELECT * FROM compaction_output_parallelism ORDER BY ts;
DROP TABLE compaction_output_parallelism;

CREATE TABLE invalid_parallelism (ts TIMESTAMP TIME INDEX)
WITH ('compaction.experimental_output_parallelism' = '0');
