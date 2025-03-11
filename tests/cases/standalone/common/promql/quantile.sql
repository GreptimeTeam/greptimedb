CREATE TABLE test (
  ts timestamp(3) time index,
  host STRING,
  idc STRING,
  val BIGINT,
  PRIMARY KEY(host, idc),
);

INSERT INTO TABLE test VALUES
    (0,     'host1', "idc1", 1),
    (0,     'host2', "idc1", 2),
    (0,     'host3', "idc2", 3),
    (0,     'host4', "idc2", 4),
    (5000,  'host1', "idc1", 5),
    (5000,  'host2', "idc1", 6),
    (5000,  'host3', "idc2", 7),
    (5000,  'host4', "idc2", 8),
    (10000, 'host1', "idc1", 9),
    (10000, 'host2', "idc1", 10),
    (10000, 'host3', "idc2", 11),
    (10000, 'host4', "idc2", 12),
    (15000, 'host1', "idc1", 13),
    (15000, 'host2', "idc1", 14),
    (15000, 'host3', "idc2", 15),
    (15000, 'host4', "idc2", 16);

TQL EVAL (0, 15, '5s') quantile(0.5, test);

TQL EVAL (0, 15, '5s') quantile(0.5, test) by (idc);

TQL EVAL (0, 15, '5s') quantile(0.5, sum(test) by (idc));

DROP TABLE test;
