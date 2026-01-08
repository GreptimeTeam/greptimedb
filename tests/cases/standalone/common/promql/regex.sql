CREATE TABLE test (
  ts timestamp(3) time index,
  host STRING,
  val BIGINT,
  PRIMARY KEY(host),
);

INSERT INTO TABLE test VALUES
    (0, '10.0.160.237:8080', 1),
    (0, '10.0.160.237:8081', 1);

SELECT * FROM test;

TQL EVAL (0, 100, '15s') test{host=~"(10.0.160.237:8080|10.0.160.237:9090)"};

TQL EVAL (0, 100, '15s') test{host=~"10\\.0\\.160\\.237:808|nonexistence"};

TQL EVAL (0, 100, '15s') test{host=~"(10\\.0\\.160\\.237:8080|10\\.0\\.160\\.237:9090)"};

-- Some radical regex optimization

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (flat_format.*) REDACTED
TQL ANALYZE VERBOSE (0, 0, '1s') test{host=~".*"};

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (flat_format.*) REDACTED
TQL ANALYZE VERBOSE (0, 0, '1s') test{host=~".+"};

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (flat_format.*) REDACTED
TQL ANALYZE VERBOSE (0, 0, '1s') test{host!~".*"};

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (flat_format.*) REDACTED
TQL ANALYZE VERBOSE (0, 0, '1s') test{host!~".+"};

DROP TABLE test;
