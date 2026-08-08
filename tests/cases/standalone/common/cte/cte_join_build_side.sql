-- Regression test for issue 6874: joining a table back to a small
-- aggregated/ordered/limited CTE derived from the same table must use the
-- bounded CTE side as the hash-join build side (CollectLeft). The decision is
-- driven by the CTE's LIMIT alone (a plan-structural bound), so the plan must
-- be identical on standalone and distributed frontends — this file is shared
-- by both sqlness environments.
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

-- Control: the plain aggregation is fully pushed below MergeScan.
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
EXPLAIN SELECT "db", count(*) AS cnt FROM cte_join_logs
GROUP BY "db" ORDER BY cnt DESC LIMIT 5;

-- Issue shape: the bounded CTE side must become the CollectLeft build side
-- (the first HashJoinExec child), with the raw scan on the probe side.
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

-- The join must still return correct results.
WITH top_dbs AS (
  SELECT "db", count(*) AS cnt FROM cte_join_logs
  GROUP BY "db" ORDER BY cnt DESC LIMIT 5
)
SELECT t."db", count(*) AS c
FROM cte_join_logs AS t
JOIN top_dbs td ON t."db" = td."db"
GROUP BY t."db" ORDER BY c DESC, t."db";

DROP TABLE cte_join_logs;
