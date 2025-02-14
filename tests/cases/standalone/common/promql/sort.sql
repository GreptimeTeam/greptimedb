CREATE TABLE test (
  ts timestamp(3) time index,
  host STRING,
  idc STRING,
  val BIGINT,
  PRIMARY KEY(host, idc),
);

INSERT INTO TABLE test VALUES
    (0,     'host1', 'idc1', 1),
    (0,     'host2', 'idc1', 2),
    (5000,  'host1', 'idc2',3),
    (5000,  'host2', 'idc2',4),
    (10000, 'host1', 'idc3',5),
    (10000, 'host2', 'idc3',6),
    (15000, 'host1', 'idc4',7),
    (15000, 'host2', 'idc4',8);


TQL EVAL (0, 15, '5s') sort(test{host="host1"});

TQL EVAL (0, 15, '5s') sort_desc(test{host="host1"});

-- SQLNESS REPLACE (\s1970-01-01T\d\d:\d\d:\d\d) timestamp
TQL EVAL (0, 15, '5s') sort(sum(test{host="host2"}) by (idc));

-- SQLNESS REPLACE (\s1970-01-01T\d\d:\d\d:\d\d) timestamp
TQL EVAL (0, 15, '5s') sort_desc(sum(test{host="host2"}) by (idc));

-- SQLNESS REPLACE (\s1970-01-01T\d\d:\d\d:\d\d) timestamp
-- SQLNESS REPLACE (\s\d\s) val
TQL EVAL (0, 15, '5s') sort_by_label(sum(test) by (idc, host), "idc", "host");

-- SQLNESS REPLACE (\s1970-01-01T\d\d:\d\d:\d\d) timestamp
-- SQLNESS REPLACE (\s\d\s) val
TQL EVAL (0, 15, '5s') sort_by_label_desc(sum(test) by (idc, host), "idc", "host");

drop table test;
