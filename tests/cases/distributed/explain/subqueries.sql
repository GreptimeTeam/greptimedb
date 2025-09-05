CREATE TABLE integers(i INTEGER, j TIMESTAMP TIME INDEX);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT * FROM integers WHERE i IN ((SELECT i FROM integers)) ORDER BY i;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT * FROM integers i1 WHERE EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i1.i;

create table other (i INTEGER, j TIMESTAMP TIME INDEX);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
explain select t.i
from (
    select * from integers join other on 1=1
) t
where t.i is not null
order by t.i desc;

INSERT INTO other SELECT i, 2 FROM integers WHERE i=(SELECT MAX(i) FROM integers);

-- Explain physical plan for DML is not supported because it looks up the table name in a way that is
-- different from normal queries. It also requires the table provider to implement the `insert_into()` method.
EXPLAIN INSERT INTO other SELECT i, 2 FROM integers WHERE i=(SELECT MAX(i) FROM integers);

drop table other;

drop table integers;

CREATE TABLE integers(i INTEGER, j TIMESTAMP TIME INDEX)
PARTITION ON COLUMNS (i) (
  i < 1000,
  i >= 1000 AND i < 2000,
  i >= 2000
);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT * FROM integers i1 WHERE EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i1.i;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT * FROM integers i1 WHERE EXISTS(SELECT count(i) FROM integers WHERE i=i1.i) ORDER BY i1.i;

DROP TABLE integers;

CREATE TABLE IF NOT EXISTS `greptime_cloud_op_logs` (
  `timestamp` TIMESTAMP(9) NOT NULL,
  `channel` STRING NULL,
  `db` STRING NULL SKIPPING INDEX WITH(false_positive_rate = '0.01', granularity = '10240', type = 'BLOOM'),
  `op_type` STRING NULL,
  TIME INDEX (`timestamp`)
)
ENGINE=mito
WITH(
  append_mode = 'true'
);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN
SELECT
  count(`tt`.`timestamp`) as t_cnt
FROM
  greptime_cloud_op_logs AS tt
WHERE
  `tt`.`timestamp` > 1756722459558000000
  and `tt`.`timestamp` <= 1756726059558000000;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT
  count(`tt`.`timestamp`) as t_cnt
FROM
  greptime_cloud_op_logs AS tt
WHERE
  `tt`.`timestamp` > 1756722459558000000
  and `tt`.`timestamp` <= 1756726059558000000;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN WITH top_50_dbs AS (
    SELECT db,count(*) as t_cnt
    FROM greptime_cloud_op_logs
    WHERE timestamp > 1756722459558000000 and timestamp <= 1756726059558000000
    GROUP BY db
    ORDER BY t_cnt DESC
    LIMIT 50
)
SELECT 
    t.db,
    count(*) as cnt,
    date_bin('1 day',timestamp) as date
FROM greptime_cloud_op_logs AS t
JOIN top_50_dbs td ON t.db = td.db
WHERE t.timestamp > 1756722459558000000 and t.timestamp <= 1756726059558000000
GROUP BY t.db, date
ORDER BY cnt desc;

DROP TABLE greptime_cloud_op_logs;