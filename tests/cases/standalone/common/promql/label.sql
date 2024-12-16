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
    (5000,  'host1', 'idc2:zone1',3),
    (5000,  'host2', 'idc2',4),
    (10000, 'host1', 'idc3:zone2',5),
    (10000, 'host2', 'idc3',6),
    (15000, 'host1', 'idc4:zone3',7),
    (15000, 'host2', 'idc4',8);

-- Missing source labels --
TQL EVAL (0, 15, '5s') label_join(test{host="host1"}, "new_host", "-");

-- dst_label is equal to source label --
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_join(test{host="host1"}, "host", "-", "host");

-- dst_label is in source labels --
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_join(test{host="host1"}, "host", "-", "idc", "host");

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_join(test{host="host1"}, "new_host", "-", "idc", "host");

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_replace(test{host="host1"}, "new_idc", "$2", "idc", "(.*):(.*)");

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_replace(test{host="host1"}, "new_idc", "idc99", "idc", "idc2.*");

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_replace(test{host="host2"}, "new_idc", "$2", "idc", "(.*):(.*)");

-- dst_label is equal to source label --
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_replace(test{host="host2"}, "idc", "$2", "idc", "(.*):(.*)");

DROP TABLE test;
