CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
);

INSERT INTO TABLE host VALUES
    (0,     'host1', 1),
    (0,     'host2', 2),
    (5000,  'host1', 3),
    (5000,  'host2', 4),
    (10000, 'host1', 5),
    (10000, 'host2', 6),
    (15000, 'host1', 7),
    (15000, 'host2', 8);

-- case only have one time series, scalar return value

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"}) + 1;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') 1 + scalar(host{host="host1"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"}) + scalar(host{host="host2"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host{host="host1"} + scalar(host{host="host2"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"}) + host{host="host2"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host + scalar(host{host="host2"});
 
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"}) + host;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(count(count(host) by (host)));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"} + scalar(host{host="host2"}));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(scalar(host{host="host2"}) + host{host="host1"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host + scalar(host{host="host2"}));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(scalar(host{host="host2"}) + host);

-- case have multiple time series, scalar return NaN

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host) + 1;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') 1 + scalar(host);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host) + scalar(host);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host + scalar(host);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host) + host;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host{host="host2"} + scalar(host);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host) + host{host="host2"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"} + scalar(host));

-- No data input in scalar
TQL EVAL (350, 360, '5s') scalar(host{host="host1"});

DELETE from host where ts = 0;

-- Under this case, InstantManipulate will input a valid record batch but output a empty record batch (because no data will be selected in this batch)
-- Test input a empty record batch to ScalarCalculate plan
TQL EVAL (0, 1600, '6m40s') scalar(host{host="host1"});

-- error case

TQL EVAL (0, 15, '5s') scalar(1 + scalar(host{host="host2"}));

TQL EVAL (0, 15, '5s') scalar(scalar(host{host="host2"}) + 1);

TQL EVAL (0, 15, '5s') scalar(scalar(host{host="host1"}) + scalar(host{host="host2"}));

Drop table host;
