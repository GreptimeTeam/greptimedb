-- Regression coverage for issues 9456/9492 (PR 9491): preserve IN/semi-join
-- results when merge-scan inputs are distributed across hash partitions.
CREATE TABLE merge_semi_data (
  ts TIMESTAMP TIME INDEX,
  key_id INT,
  is_match BOOLEAN,
  PRIMARY KEY (key_id)
)
PARTITION ON COLUMNS (key_id) (
  key_id < 10,
  key_id >= 10 AND key_id < 20,
  key_id >= 20 AND key_id < 30,
  key_id >= 30
)
ENGINE=mito;

-- Three keys in each of four ranges; each key has a false row then a true row.
INSERT INTO merge_semi_data VALUES
  ('2026-10-09 00:00:00', 1, false), ('2026-10-09 00:00:01', 1, true),
  ('2026-10-09 00:00:00', 2, false), ('2026-10-09 00:00:01', 2, true),
  ('2026-10-09 00:00:00', 3, false), ('2026-10-09 00:00:01', 3, true),
  ('2026-10-09 00:00:00', 11, false), ('2026-10-09 00:00:01', 11, true),
  ('2026-10-09 00:00:00', 12, false), ('2026-10-09 00:00:01', 12, true),
  ('2026-10-09 00:00:00', 13, false), ('2026-10-09 00:00:01', 13, true),
  ('2026-10-09 00:00:00', 21, false), ('2026-10-09 00:00:01', 21, true),
  ('2026-10-09 00:00:00', 22, false), ('2026-10-09 00:00:01', 22, true),
  ('2026-10-09 00:00:00', 23, false), ('2026-10-09 00:00:01', 23, true),
  ('2026-10-09 00:00:00', 31, false), ('2026-10-09 00:00:01', 31, true),
  ('2026-10-09 00:00:00', 32, false), ('2026-10-09 00:00:01', 32, true),
  ('2026-10-09 00:00:00', 33, false), ('2026-10-09 00:00:01', 33, true);

-- Check the plan retains the partitioned left-semi join and hash keys (N=4).
-- SQLNESS REPLACE (peers.*) peers=REDACTED
EXPLAIN SELECT key_id, MIN(ts) AS first_ts
FROM merge_semi_data
WHERE key_id IN (
  SELECT key_id FROM merge_semi_data WHERE is_match = true
)
GROUP BY key_id ORDER BY key_id;

-- Every key matches and reports its earliest (false-row) timestamp.
SELECT key_id, MIN(ts) AS first_ts
FROM merge_semi_data
WHERE key_id IN (
  SELECT key_id FROM merge_semi_data WHERE is_match = true
)
GROUP BY key_id ORDER BY key_id;

DROP TABLE merge_semi_data;
