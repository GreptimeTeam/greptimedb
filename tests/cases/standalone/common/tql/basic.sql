CREATE TABLE test(i DOUBLE, j TIMESTAMP TIME INDEX, k STRING PRIMARY KEY);

-- insert two points at 1ms and one point at 2ms
INSERT INTO test VALUES (1, 1, "a"), (1, 1, "b"), (2, 2, "a");

-- SQLNESS SORT_RESULT 2 1
-- evaluate at 0s, 5s and 10s. No point at 0s.
TQL EVAL (0, 10, '5s') test;

-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 10, '5s') {__name__="test"};

-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 10, '5s') test{__schema__="public"};

-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 10, '5s') test{__schema__="greptime_private"};

-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 10, '5s') test{__database__="public"};

-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 10, '5s') test{__database__="greptime_private"};

-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 10, '5s') {__name__="test", __field__="i"};

-- NOT SUPPORTED: `__name__` matcher without equal condition
TQL EVAL (0, 10, '5s') {__name__!="test"};

TQL EVAL (0, 10, '5s') {__name__=~"test"};

-- the point at 1ms will be shadowed by the point at 2ms
TQL EVAL (0, 10, '5s') test{k="a"};

TQL EVAL (0, 10, '1s', '2s') test{k="a"};

-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 10, '0.5') test;

-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 10, 0.5) test;


TQL EVAL ('1970-01-01T00:00:00'::timestamp, '1970-01-01T00:00:00'::timestamp + '10 seconds'::interval, '1s') test{k="a"};

TQL EVAL (now() - now(), now() -  (now() - '10 seconds'::interval), '1s')  test{k="a"};

DROP TABLE test;

CREATE TABLE test (`Field_I` DOUBLE, `Ts_J` TIMESTAMP TIME INDEX, `Tag_K` STRING PRIMARY KEY);

INSERT INTO test VALUES (1, 1, "a"), (1, 1, "b"), (2, 2, "a");

-- SQLNESS SORT_RESULT 2 1
TQL EVAL (0, 10, '5s') test{__field__="Field_I"};

TQL EVAL (0, 10, '5s') test{__field__="field_i"};

drop table test;

CREATE TABLE metrics(val DOUBLE, ts TIMESTAMP TIME INDEX, host STRING PRIMARY KEY);

-- Insert sample data with fixed timestamps for predictable testing
INSERT INTO metrics VALUES 
    (10.0, '2024-01-01 00:00:00', 'host1'),
    (15.0, '2024-01-01 06:00:00', 'host1'),
    (20.0, '2024-01-01 12:00:00', 'host1'),
    (25.0, '2024-01-01 18:00:00', 'host1'),
    (30.0, '2024-01-02 00:00:00', 'host1'),
    (35.0, '2024-01-02 06:00:00', 'host1'),
    (40.0, '2024-01-02 12:00:00', 'host1'),
    (12.0, '2024-01-01 02:00:00', 'host2'),
    (18.0, '2024-01-01 08:00:00', 'host2'),
    (22.0, '2024-01-01 14:00:00', 'host2');

-- Test TQL with date_trunc function
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (date_trunc('day', '2024-01-01 10:00:00'::timestamp), 
          date_trunc('day', '2024-01-02 10:00:00'::timestamp), 
          '6h') last_over_time(metrics[12h]);

-- Test TQL with date_trunc and interval arithmetic (now() - interval '1' day pattern)
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (date_trunc('day', '2024-01-02 00:00:00'::timestamp) - interval '1' day,
          date_trunc('day', '2024-01-02 00:00:00'::timestamp),
          '4h') metrics{host="host1"};

-- Test TQL with hour-based queries
-- SQLNESS SORT_RESULT 2 1
TQL EVAL ('2024-01-01 06:00:00'::timestamp,
          '2024-01-01 18:00:00'::timestamp,
          '3h', 
          '6h') metrics;

-- Test TQL with complex interval arithmetic
-- SQLNESS SORT_RESULT 2 1
TQL EVAL ('2024-01-01 00:00:00'::timestamp + interval '12' hour,
          '2024-01-02 00:00:00'::timestamp - interval '6' hour,
          '6h', 
          '6h') metrics{host="host2"};

DROP TABLE metrics;
