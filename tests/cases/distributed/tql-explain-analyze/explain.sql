CREATE TABLE test(i DOUBLE, j TIMESTAMP TIME INDEX, k STRING PRIMARY KEY);

-- insert two points at 1ms and one point at 2ms
INSERT INTO test VALUES (1, 1, "a"), (1, 1, "b"), (2, 2, "a");

-- explain at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
TQL EXPLAIN (0, 10, '5s') test;

DROP TABLE test;

CREATE TABLE host_load1 (
  ts TIMESTAMP(3) NOT NULL,
  collector STRING NULL,
  host STRING NULL,
  val DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY (collector, host)
);

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
TQL EXPLAIN host_load1{__field__="val"};

DROP TABLE host_load1;
