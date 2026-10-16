-- Regression coverage for issues 9456/9492 (PR 9491): preserve merge-scan
-- results for a pruned left join over partitioned append data.
CREATE TABLE merge_left_data (
  k STRING,
  kind STRING,
  ts TIMESTAMP TIME INDEX,
  PRIMARY KEY (k, kind)
)
PARTITION ON COLUMNS (k) (
  k < '1', k >= '1' AND k < '2', k >= '2' AND k < '3',
  k >= '3' AND k < '4', k >= '4' AND k < '5', k >= '5' AND k < '6',
  k >= '6' AND k < '7', k >= '7' AND k < '8', k >= '8' AND k < '9',
  k >= '9' AND k < 'a', k >= 'a' AND k < 'b', k >= 'b' AND k < 'c',
  k >= 'c' AND k < 'd', k >= 'd' AND k < 'e', k >= 'e' AND k < 'f',
  k >= 'f'
)
ENGINE=mito WITH (append_mode = 'true');

INSERT INTO merge_left_data VALUES
  ('0', 'root', '2026-10-09 00:00:00'),
  ('1', 'root', '2026-10-09 00:00:00'),
  ('2', 'root', '2026-10-09 00:00:00'),
  ('3', 'root', '2026-10-09 00:00:00'),
  ('4', 'root', '2026-10-09 00:00:00'),
  ('5', 'root', '2026-10-09 00:00:00'),
  ('6', 'root', '2026-10-09 00:00:00'),
  ('7', 'root', '2026-10-09 00:00:00'),
  ('8', 'root', '2026-10-09 00:00:00'),
  ('9', 'root', '2026-10-09 00:00:00'),
  ('a', 'root', '2026-10-09 00:00:00'),
  ('b', 'root', '2026-10-09 00:00:00'),
  ('c', 'root', '2026-10-09 00:00:00'),
  ('d', 'root', '2026-10-09 00:00:00'),
  ('e', 'root', '2026-10-09 00:00:00'),
  ('f', 'root', '2026-10-09 00:00:00'),
  ('z', 'root', '2026-10-09 00:00:00'),
  ('0', 'child', '2026-10-09 00:00:00'), ('0', 'child', '2026-10-09 00:00:00'), ('0', 'child', '2026-10-09 00:00:00'),
  ('1', 'child', '2026-10-09 00:00:00'), ('1', 'child', '2026-10-09 00:00:00'), ('1', 'child', '2026-10-09 00:00:00'),
  ('2', 'child', '2026-10-09 00:00:00'), ('2', 'child', '2026-10-09 00:00:00'), ('2', 'child', '2026-10-09 00:00:00'),
  ('3', 'child', '2026-10-09 00:00:00'), ('3', 'child', '2026-10-09 00:00:00'), ('3', 'child', '2026-10-09 00:00:00'),
  ('4', 'child', '2026-10-09 00:00:00'), ('4', 'child', '2026-10-09 00:00:00'), ('4', 'child', '2026-10-09 00:00:00'),
  ('5', 'child', '2026-10-09 00:00:00'), ('5', 'child', '2026-10-09 00:00:00'), ('5', 'child', '2026-10-09 00:00:00'),
  ('6', 'child', '2026-10-09 00:00:00'), ('6', 'child', '2026-10-09 00:00:00'), ('6', 'child', '2026-10-09 00:00:00'),
  ('7', 'child', '2026-10-09 00:00:00'), ('7', 'child', '2026-10-09 00:00:00'), ('7', 'child', '2026-10-09 00:00:00'),
  ('8', 'child', '2026-10-09 00:00:00'), ('8', 'child', '2026-10-09 00:00:00'), ('8', 'child', '2026-10-09 00:00:00'),
  ('9', 'child', '2026-10-09 00:00:00'), ('9', 'child', '2026-10-09 00:00:00'), ('9', 'child', '2026-10-09 00:00:00'),
  ('a', 'child', '2026-10-09 00:00:00'), ('a', 'child', '2026-10-09 00:00:00'), ('a', 'child', '2026-10-09 00:00:00'),
  ('b', 'child', '2026-10-09 00:00:00'), ('b', 'child', '2026-10-09 00:00:00'), ('b', 'child', '2026-10-09 00:00:00'),
  ('c', 'child', '2026-10-09 00:00:00'), ('c', 'child', '2026-10-09 00:00:00'), ('c', 'child', '2026-10-09 00:00:00'),
  ('d', 'child', '2026-10-09 00:00:00'), ('d', 'child', '2026-10-09 00:00:00'), ('d', 'child', '2026-10-09 00:00:00'),
  ('e', 'child', '2026-10-09 00:00:00'), ('e', 'child', '2026-10-09 00:00:00'), ('e', 'child', '2026-10-09 00:00:00'),
  ('f', 'child', '2026-10-09 00:00:00'), ('f', 'child', '2026-10-09 00:00:00'), ('f', 'child', '2026-10-09 00:00:00');

-- Full join: 17 roots, 16 matched roots, 48 child rows.
SELECT r.k, c.n
FROM (SELECT k FROM merge_left_data WHERE kind = 'root') r
LEFT JOIN (
  SELECT k, COUNT(*) AS n FROM merge_left_data WHERE kind = 'child' GROUP BY k
) c ON r.k = c.k
ORDER BY r.k;

-- Assert partitioned hash join and both hash repartition keys at SQLness N=4.
-- SQLNESS REPLACE (peers.*) peers=REDACTED
EXPLAIN SELECT r.k, c.n
FROM (SELECT k FROM merge_left_data WHERE kind = 'root' AND k >= '1' AND k < '5') r
LEFT JOIN (
  SELECT k, COUNT(*) AS n FROM merge_left_data
  WHERE kind = 'child' AND k < '4' GROUP BY k
) c ON r.k = c.k
ORDER BY r.k;

-- Four roots, three matches, nine child rows; key 4 remains with NULL n.
SELECT r.k, c.n
FROM (SELECT k FROM merge_left_data WHERE kind = 'root' AND k >= '1' AND k < '5') r
LEFT JOIN (
  SELECT k, COUNT(*) AS n FROM merge_left_data
  WHERE kind = 'child' AND k < '4' GROUP BY k
) c ON r.k = c.k
ORDER BY r.k;

DROP TABLE merge_left_data;
