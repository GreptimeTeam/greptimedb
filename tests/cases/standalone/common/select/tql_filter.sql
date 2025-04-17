create table t1 (a string primary key, b timestamp time index, c double);

insert into t1 values ("a", 1000, 1.0), ("b", 2000, 2.0), ("c", 3000, 3.0);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze (1, 3, '1s') t1{ a = "a" };

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze (1, 3, '1s') t1{ a =~ ".*" };

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze (1, 3, '1s') t1{ a =~ "a.*" };

drop table t1;

create table t2 (a string primary key, b timestamp time index, c double);

INSERT INTO TABLE t2 VALUES
    ('10.0.160.237:8080', 0, 1),
    ('10.0.160.237:8081', 0, 1),
    ('20.0.10.237:8081', 0, 1),
    ('abcx', 0, 1),
    ('xabc', 0, 1);

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 0, '1s') t2{a=~"10"};

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 0, '1s') t2{a=~"10.*"};

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 0, '1s') t2{a=~".*10.*"};

drop table t2;
