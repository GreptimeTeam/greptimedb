-- test single value table --
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
    (5000,  'host1', "idc1", 1),
    (5000,  'host2', "idc1", 4),
    (5000,  'host3', "idc2", 1),
    (10000, 'host1', "idc1", 3),
    (10000, 'host2', "idc1", 5),
    (10000, 'host3', "idc2", 3),
    (15000,  'host1', "idc1", 1),
    (15000,  'host2', "idc1", 2),
    (15000,  'host3', "idc2", 3);

TQL EVAL (0, 15, '5s') topk(1, test);

TQL EVAL (0, 15, '5s') topk(3, test);

TQL EVAL (0, 15, '5s') topk(1, sum(test) by (idc));

TQL EVAL (0, 15, '5s') topk(2, sum(test) by (idc));

TQL EVAL (0, 15, '5s') bottomk(1, test);

TQL EVAL (0, 15, '5s') bottomk(3, test);

TQL EVAL (0, 15, '5s') bottomk(1, sum(test) by (idc));

TQL EVAL (0, 15, '5s') bottomk(2, sum(test) by (idc));


DROP table test;

-- test multi-values table --

CREATE TABLE test (
  ts timestamp(3) time index,
  host STRING,
  idc STRING,
  cpu BIGINT,
  mem BIGINT,
  PRIMARY KEY(host, idc),
);

INSERT INTO TABLE test VALUES
    (0,     'host1', "idc1", 1, 3),
    (0,     'host2', "idc1", 2, 2),
    (0,     'host3', "idc2", 3, 1),
    (5000,  'host1', "idc1", 1, 1),
    (5000,  'host2', "idc1", 4, 4),
    (5000,  'host3', "idc2", 1, 1),
    (10000, 'host1', "idc1", 3, 3),
    (10000, 'host2', "idc1", 5, 5),
    (10000, 'host3', "idc2", 3, 3),
    (15000,  'host1', "idc1", 1, 3),
    (15000,  'host2', "idc1", 2, 2),
    (15000,  'host3', "idc2", 3, 1);

TQL EVAL (0, 15, '5s') topk(1, test);

TQL EVAL (0, 15, '5s') topk(1, sum(test{__field__='cpu'}) by (idc));

TQL EVAL (0, 15, '5s') topk(1, sum(test{__field__='mem'}) by (idc));

TQL EVAL (0, 15, '5s') bottomk(1, sum(test{__field__='cpu'}) by (idc));

TQL EVAL (0, 15, '5s') bottomk(1, sum(test{__field__='mem'}) by (idc));

DROP table test;
