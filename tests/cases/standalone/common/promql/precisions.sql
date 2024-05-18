CREATE TABLE host_sec (
  ts timestamp(0) time index,
  host STRING PRIMARY KEY,
  val DOUBLE,
);

INSERT INTO TABLE host_sec VALUES
    (0,  'host1', 1),
    (0,  'host2', 2),
    (5,  'host1', 3),
    (5,  'host2', 4),
    (10, 'host1', 5),
    (10, 'host2', 6),
    (15, 'host1', 7),
    (15, 'host2', 8);

CREATE TABLE host_micro (
  ts timestamp(6) time index,
  host STRING PRIMARY KEY,
  val DOUBLE,
);

INSERT INTO TABLE host_micro VALUES
    (0,        'host1', 1),
    (0,        'host2', 2),
    (5000000,  'host1', 3),
    (5000000,  'host2', 4),
    (10000000, 'host1', 5),
    (10000000, 'host2', 6),
    (15000000, 'host1', 7),
    (15000000, 'host2', 8);

-- Test on Timestamps of different precisions

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host_sec{host="host1"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') avg_over_time(host_sec{host="host1"}[5s]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host_micro{host="host1"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') avg_over_time(host_micro{host="host1"}[5s]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host_sec{host="host1"} + host_micro{host="host1"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') avg_over_time(host_sec{host="host1"}[5s]) + avg_over_time(host_micro{host="host1"}[5s]);

DROP TABLE host_sec;

DROP TABLE host_micro;
