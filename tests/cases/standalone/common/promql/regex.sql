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

DROP TABLE test;
