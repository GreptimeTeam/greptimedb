-- Regression test for TSID-backed PromQL binary joins on metric-engine tables.
-- Default arithmetic and comparison joins should use `__tsid` when matching is the
-- default one-to-one case. Label modifiers still have to stay label-based.

CREATE TABLE tsid_binary_join_physical (
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
) ENGINE = metric WITH ("physical_metric_table" = "");

CREATE TABLE tsid_binary_join_left (
  host STRING NULL,
  job STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(host, job),
)
ENGINE = metric
WITH(
  on_physical_table = 'tsid_binary_join_physical'
);

CREATE TABLE tsid_binary_join_right (
  host STRING NULL,
  job STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(host, job),
)
ENGINE = metric
WITH(
  on_physical_table = 'tsid_binary_join_physical'
);

CREATE TABLE tsid_binary_join_right_by_job (
  job STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(job),
)
ENGINE = metric
WITH(
  on_physical_table = 'tsid_binary_join_physical'
);

CREATE TABLE tsid_binary_join_third (
  host STRING NULL,
  job STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(host, job),
)
ENGINE = metric
WITH(
  on_physical_table = 'tsid_binary_join_physical'
);

INSERT INTO tsid_binary_join_left (host, job, ts, greptime_value) VALUES
  ('host1', 'job1', 0, 12),
  ('host2', 'job2', 0, 18),
  ('host1', 'job1', 5000, 15),
  ('host2', 'job2', 5000, 21);

INSERT INTO tsid_binary_join_right (host, job, ts, greptime_value) VALUES
  ('host1', 'job1', 0, 3),
  ('host2', 'job2', 0, 6),
  ('host1', 'job1', 5000, 5),
  ('host2', 'job2', 5000, 7);

INSERT INTO tsid_binary_join_right_by_job (job, ts, greptime_value) VALUES
  ('job1', 0, 3),
  ('job2', 0, 6),
  ('job1', 5000, 5),
  ('job2', 5000, 7);

INSERT INTO tsid_binary_join_third (host, job, ts, greptime_value) VALUES
  ('host1', 'job1', 0, 2),
  ('host2', 'job2', 0, 3),
  ('host1', 'job1', 5000, 4),
  ('host2', 'job2', 5000, 6);

-- Default vector-vector arithmetic should join on `__tsid` and time index.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE Hash\(\[__tsid@1,\sts@2\],.* Hash([__tsid@1, ts@2],REDACTED
-- SQLNESS REPLACE Hash\(\[__tsid@3,\sts@4\],.* Hash([__tsid@3, ts@4],REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE FilterExec:\sDynamicFilter\s\[[^\r\n|]*\] FilterExec: DynamicFilter [ REDACTED ]
TQL ANALYZE (0, 5, '5s') tsid_binary_join_left / tsid_binary_join_right;

-- Repeated operands in a safe arithmetic island should be planned once and reused
-- in the final projection.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE Hash\(\[__tsid@1,\sts@2\],.* Hash([__tsid@1, ts@2],REDACTED
-- SQLNESS REPLACE Hash\(\[__tsid@3,\sts@4\],.* Hash([__tsid@3, ts@4],REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE FilterExec:\sDynamicFilter\s\[[^\r\n|]*\] FilterExec: DynamicFilter [ REDACTED ]
TQL ANALYZE (0, 5, '5s') (tsid_binary_join_left + tsid_binary_join_right) / tsid_binary_join_left;

-- A larger arithmetic island should still plan each distinct vector selector only once
-- while reusing the repeated left operand in multiple branches.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE Hash\(\[[^\]]+\],.* Hash([REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE FilterExec:\sDynamicFilter\s\[[^\r\n|]*\] FilterExec: DynamicFilter [ REDACTED ]
TQL ANALYZE (0, 5, '5s') ((tsid_binary_join_left + tsid_binary_join_right) * (tsid_binary_join_left - tsid_binary_join_third)) / (tsid_binary_join_left + 2);

-- Label modifiers must disable the TSID shortcut and keep matching on the remaining labels.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE Hash\(\[job@1,\sts@2\],.* Hash([job@1, ts@2],REDACTED
-- SQLNESS REPLACE Hash\(\[job@2,\sts@4\],.* Hash([job@2, ts@4],REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE FilterExec:\sDynamicFilter\s\[[^\r\n|]*\] FilterExec: DynamicFilter [ REDACTED ]
TQL ANALYZE (0, 5, '5s') tsid_binary_join_left / ignoring(host) tsid_binary_join_right;

-- `on(job)` must stay label-based when only the left side has extra row-key labels.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE Hash\(\[job@1,\sts@2\],.* Hash([job@1, ts@2],REDACTED
-- SQLNESS REPLACE Hash\(\[job@1,\sts@3\],.* Hash([job@1, ts@3],REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE FilterExec:\sDynamicFilter\s\[[^\r\n|]*\] FilterExec: DynamicFilter [ REDACTED ]
TQL ANALYZE (0, 5, '5s') tsid_binary_join_left / on(job) tsid_binary_join_right_by_job;

-- Comparison filters can join on `__tsid`, but the filtered result must still behave like
-- a regular derived vector downstream.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE Hash\(\[__tsid@1,\sts@2\],.* Hash([__tsid@1, ts@2],REDACTED
-- SQLNESS REPLACE Hash\(\[__tsid@3,\sts@4\],.* Hash([__tsid@3, ts@4],REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE FilterExec:\sDynamicFilter\s\[[^\r\n|]*\] FilterExec: DynamicFilter [ REDACTED ]
TQL ANALYZE (0, 5, '5s') tsid_binary_join_left > tsid_binary_join_right;

-- `bool` comparison should follow the same TSID-backed matching path.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE Hash\(\[__tsid@1,\sts@2\],.* Hash([__tsid@1, ts@2],REDACTED
-- SQLNESS REPLACE Hash\(\[__tsid@3,\sts@4\],.* Hash([__tsid@3, ts@4],REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE FilterExec:\sDynamicFilter\s\[[^\r\n|]*\] FilterExec: DynamicFilter [ REDACTED ]
TQL ANALYZE (0, 5, '5s') tsid_binary_join_left > bool tsid_binary_join_right;

-- Comparison filters are a barrier for binary island coalescing because they filter the
-- vector domain instead of producing only a value expression.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE Hash\(\[[^\]]+\],.* Hash([REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE FilterExec:\sDynamicFilter\s\[[^\r\n|]*\] FilterExec: DynamicFilter [ REDACTED ]
TQL ANALYZE (0, 5, '5s') ((tsid_binary_join_left > tsid_binary_join_right) / tsid_binary_join_left) * 100;

-- Bool comparisons are intentionally outside the first island optimization version.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE Hash\(\[[^\]]+\],.* Hash([REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE FilterExec:\sDynamicFilter\s\[[^\r\n|]*\] FilterExec: DynamicFilter [ REDACTED ]
TQL ANALYZE (0, 5, '5s') ((tsid_binary_join_left > bool tsid_binary_join_right) + tsid_binary_join_left) / tsid_binary_join_left;

-- Set operators are a barrier because they have distinct matching and output-domain
-- semantics.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE Hash\(\[[^\]]+\],.* Hash([REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE FilterExec:\sDynamicFilter\s\[[^\r\n|]*\] FilterExec: DynamicFilter [ REDACTED ]
TQL ANALYZE (0, 5, '5s') (tsid_binary_join_left or tsid_binary_join_right) / tsid_binary_join_left;

-- Group modifiers are many-to-one/one-to-many matching barriers and must stay on the
-- legacy path.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE Hash\(\[[^\]]+\],.* Hash([REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE FilterExec:\sDynamicFilter\s\[[^\r\n|]*\] FilterExec: DynamicFilter [ REDACTED ]
TQL ANALYZE (0, 5, '5s') (tsid_binary_join_left / ignoring(host) group_left tsid_binary_join_right) / tsid_binary_join_left;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 5, '5s') tsid_binary_join_left / tsid_binary_join_right;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 5, '5s') tsid_binary_join_left / on(job) tsid_binary_join_right_by_job;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 5, '5s') (tsid_binary_join_left + tsid_binary_join_right) / tsid_binary_join_left;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 5, '5s') ((tsid_binary_join_left + tsid_binary_join_right) * (tsid_binary_join_left - tsid_binary_join_third)) / (tsid_binary_join_left + 2);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 5, '5s') ((tsid_binary_join_left > tsid_binary_join_right) / tsid_binary_join_left) * 100;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 5, '5s') ((tsid_binary_join_left > bool tsid_binary_join_right) + tsid_binary_join_left) / tsid_binary_join_left;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 5, '5s') (tsid_binary_join_left or tsid_binary_join_right) / tsid_binary_join_left;

-- Range functions are outside the first island version; the range selector subtree must
-- remain a barrier.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 5, '5s') rate(tsid_binary_join_left[5s]) / tsid_binary_join_left;

DROP TABLE tsid_binary_join_third;
DROP TABLE tsid_binary_join_right_by_job;
DROP TABLE tsid_binary_join_right;
DROP TABLE tsid_binary_join_left;
DROP TABLE tsid_binary_join_physical;
