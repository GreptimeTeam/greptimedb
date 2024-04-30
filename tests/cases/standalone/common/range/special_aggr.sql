CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
  addon BIGINT,
);

INSERT INTO TABLE host VALUES
    (0,     'host1', 0, 1),
    (1000,  'host1', 1, 2),
    (2000,  'host1', 2, 3),

    (5000,  'host1', null, 4),
    (6000,  'host1', null, 5),
    (7000,  'host1', null, 6),

    (10000, 'host1', null, 7),
    (11000, 'host1', 4, 8),
    (12000, 'host1', 5, 9),

    (15000, 'host1', 6, 10),
    (16000, 'host1', null, 11),
    (17000, 'host1', 7, 12),

    (20000, 'host1', 8, 13),
    (21000, 'host1', 9, 14),
    (22000, 'host1', null, 15),

    (0,     'host2', 0, 16),
    (1000,  'host2', 1, 17),
    (2000,  'host2', 2, 18),

    (5000,  'host2', null, 19),
    (6000,  'host2', null, 20),
    (7000,  'host2', null, 21),

    (10000, 'host2', null, 22),
    (11000, 'host2', 4, 23),
    (12000, 'host2', 5, 24),

    (15000, 'host2', 6, 25),
    (16000, 'host2', null, 26),
    (17000, 'host2', 7, 27),

    (20000, 'host2', 8, 28),
    (21000, 'host2', 9, 29),
    (22000, 'host2', null, 30);

SELECT ts, host, first_value(val) RANGE '5s', last_value(val) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, first_value(addon ORDER BY val DESC) RANGE '5s', last_value(addon ORDER BY val DESC) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, first_value(addon ORDER BY val DESC NULLS LAST) RANGE '5s', last_value(addon ORDER BY val DESC NULLS LAST) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, first_value(addon ORDER BY val ASC) RANGE '5s', last_value(addon ORDER BY val ASC) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, first_value(addon ORDER BY val ASC NULLS FIRST) RANGE '5s', last_value(addon ORDER BY val ASC NULLS FIRST) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, first_value(addon ORDER BY val ASC, ts ASC) RANGE '5s', last_value(addon ORDER BY val ASC, ts ASC) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, count(val) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, count(distinct val) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, count(*) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, count(1) RANGE '5s' as abc FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, count(distinct *) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

-- Test error first_value/last_value

SELECT ts, host, first_value(val, val) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

DROP TABLE host;

-- Test first_value/last_value will execute sort

CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
  addon BIGINT,
);

INSERT INTO TABLE host VALUES
    (0,     'host1', 0, 3),
    (1000,  'host1', 1, 2),
    (2000,  'host1', 2, 1);

SELECT ts, first_value(val ORDER BY addon ASC) RANGE '5s', last_value(val ORDER BY addon ASC) RANGE '5s' FROM host ALIGN '5s';

DROP TABLE host;
