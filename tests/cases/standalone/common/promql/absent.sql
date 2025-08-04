create table t (
  ts timestamp(3) time index,
  job STRING,
  instance STRING,
  val DOUBLE,
  PRIMARY KEY(job, instance),
);

insert into t values
    (0, 'job1', 'instance1', 1),
    (0, 'job2', 'instance2', 2),
    (5000, 'job1', 'instance3',3),
    (5000, 'job2', 'instance4',4),
    (10000, 'job1', 'instance5',5),
    (10000, 'job2', 'instance6',6),
    (15000, 'job1', 'instance7',7),
    (15000, 'job2', 'instance8',8);

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 15, '5s') absent(t{job="job1"});

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 15, '5s') absent(t{job="job2"});

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 15, '5s') absent(t{job="job3"});

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 15, '5s') absent(nonexistent_table);

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 15, '5s') absent(t{job="nonexistent_job"});

-- SQLNESS SORT_RESULT 3 1
tql eval (1000, 1000, '1s') absent(t{job="job1"});

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 15, '5s') absent(t{job="nonexistent_job1", job="nonexistent_job2"});

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 15, '5s') absent(t{job=~"nonexistent_job1", job!="nonexistent_job2"});

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 15, '5s') sum(t{job="job2"});

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 15, '5s') absent(sum(t{job="job2"}));

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 15, '5s') absent(sum(t{job="job3"}));

drop table t;
