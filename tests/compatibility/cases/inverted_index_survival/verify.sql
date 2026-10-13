-- 1. Index metadata written by the old binary must survive.
SHOW CREATE TABLE t_inverted_index_survival;

-- 2. Rows written by the old binary must still be readable.
SELECT host, msg, cpu
FROM t_inverted_index_survival
ORDER BY host, ts;

-- 3. Filtered read on the column indexed by the old binary.
SELECT host, msg
FROM t_inverted_index_survival
WHERE msg = 'first event'
ORDER BY host, ts;

-- 4. The new binary must still be able to write into the old table.
INSERT INTO t_inverted_index_survival VALUES
  ('h3', 'third event', 'third note', '2026-07-07 00:02:00+0000', 3.0);

SELECT host, msg, cpu
FROM t_inverted_index_survival
ORDER BY host, ts;

-- 5. Enabling an index on an existing column must keep SSTs written before the
--    index existed readable (regression guard for #9535). 'first note' and
--    'second note' live in the SST written by the old binary, which carries
--    inverted indexes for host/msg but not for msg2.
ALTER TABLE t_inverted_index_survival MODIFY COLUMN msg2 SET INVERTED INDEX;

SHOW CREATE TABLE t_inverted_index_survival;

SELECT host, msg2
FROM t_inverted_index_survival
WHERE msg2 = 'first note'
ORDER BY host, ts;

SELECT host, msg2
FROM t_inverted_index_survival
WHERE msg2 = 'second note'
ORDER BY host, ts;

-- A non-matching predicate must still return nothing.
SELECT host, msg2
FROM t_inverted_index_survival
WHERE msg2 = 'no such note'
ORDER BY host, ts;

-- 6. The index can still be removed afterwards.
ALTER TABLE t_inverted_index_survival MODIFY COLUMN msg2 UNSET INVERTED INDEX;

SHOW CREATE TABLE t_inverted_index_survival;