-- run PromQL on non-typical prometheus table schema
CREATE TABLE IF NOT EXISTS `cpu_usage` (
  `job` STRING NULL,
  `value` DOUBLE NULL,
  `ts` TIMESTAMP(9) NOT NULL,
  TIME INDEX (`ts`),
  PRIMARY KEY (`job`) 
)
ENGINE=mito
WITH(
  merge_mode = 'last_non_null'
);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
TQL analyze (0, 10, '1s')  sum by(job) (irate(cpu_usage{job="fire"}[5s])) / 1e9;

drop table `cpu_usage`;
