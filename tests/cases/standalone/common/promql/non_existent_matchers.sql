CREATE TABLE test (
   ts timestamp(3) time index,
   host STRING,
   val BIGINT,
   PRIMARY KEY(host),
 );

INSERT INTO TABLE test VALUES
     (0, 'host1', 1),
     (0, 'host2', 2);

SELECT * FROM test;

-- test the non-existent matchers --
TQL EVAL (0, 15, '5s') test{job=~"host1|host3"};

DROP TABLE test;
