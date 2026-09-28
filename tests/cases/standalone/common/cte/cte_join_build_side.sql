-- Regression test for issue 6874: a bounded CTE derived from the same table
-- must become the hash-join build side in both standalone and distributed mode.
CREATE TABLE cte_join_logs (
  ts TIMESTAMP TIME INDEX,
  "db" STRING,
  val DOUBLE,
) WITH (append_mode = 'true');

INSERT INTO cte_join_logs VALUES
  ('2024-01-01 00:00:00', 'a', 1.0),
  ('2024-01-01 00:00:01', 'a', 2.0),
  ('2024-01-01 00:00:02', 'b', 3.0),
  ('2024-01-01 00:00:03', 'b', 4.0),
  ('2024-01-01 00:00:04', 'c', 5.0),
  ('2024-01-01 00:00:05', 'c', 6.0);

-- Control: the plain Top-N aggregation should remain below MergeScan.
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
EXPLAIN SELECT "db", count(*) AS cnt
FROM cte_join_logs
GROUP BY "db"
ORDER BY cnt DESC
LIMIT 5;

-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
EXPLAIN WITH top_dbs AS (
  SELECT "db", count(*) AS cnt FROM cte_join_logs
  GROUP BY "db" ORDER BY cnt DESC LIMIT 5
)
SELECT t."db", count(*) AS c
FROM cte_join_logs AS t
JOIN top_dbs td ON t."db" = td."db"
GROUP BY t."db" ORDER BY c DESC, t."db";

WITH top_dbs AS (
  SELECT "db", count(*) AS cnt FROM cte_join_logs
  GROUP BY "db" ORDER BY cnt DESC LIMIT 5
)
SELECT t."db", count(*) AS c
FROM cte_join_logs AS t
JOIN top_dbs td ON t."db" = td."db"
GROUP BY t."db" ORDER BY c DESC, t."db";

DROP TABLE cte_join_logs;
