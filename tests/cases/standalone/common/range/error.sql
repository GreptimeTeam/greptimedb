CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
);

INSERT INTO TABLE host VALUES
    (0,     'host1', 0),
    (5000,  'host1', null),
    (10000, 'host1', 1),
    (15000, 'host1', null),
    (20000, 'host1', 2),
    (0,     'host2', 3),
    (5000,  'host2', null),
    (10000, 'host2', 4),
    (15000, 'host2', null),
    (20000, 'host2', 5);

-- Test Invalid cases

-- 1. error timestamp

SELECT min(val) RANGE 'not_time' FROM host ALIGN '5s';

SELECT min(val) RANGE '5s' FROM host ALIGN 'not_time';

-- 2.1 no range param

SELECT min(val) FROM host ALIGN '5s';

SELECT 1 FROM host ALIGN '5s';

SELECT min(val) RANGE '10s', max(val) FROM host ALIGN '5s';

SELECT min(val) * 2 RANGE '10s' FROM host ALIGN '5s';

SELECT 1 RANGE '10s' FILL NULL FROM host ALIGN '1h' FILL NULL;

-- 2.2 no align param

SELECT min(val) RANGE '5s' FROM host;

-- 2.3 type mismatch

SELECT covar(ceil(val), floor(val)) RANGE '20s' FROM host ALIGN '10s';

-- 2.4 nest query

SELECT min(max(val) RANGE '20s') RANGE '20s' FROM host ALIGN '10s';

-- 2.5 wrong Aggregate

SELECT rank() OVER (PARTITION BY host ORDER BY ts DESC) RANGE '10s' FROM host ALIGN '5s';

-- 2.6 invalid fill

SELECT min(val) RANGE '5s', min(val) RANGE '5s' FILL NULL FROM host ALIGN '5s';

SELECT min(val) RANGE '5s' FROM host ALIGN '5s' FILL 3.0;

-- 2.7 zero align/range

SELECT min(val) RANGE '5s' FROM host ALIGN '0s';

SELECT min(val) RANGE '0s' FROM host ALIGN '5s';

SELECT min(val) RANGE '5s' FROM host ALIGN (INTERVAL '0' day);

SELECT min(val) RANGE (INTERVAL '0' day) FROM host ALIGN '5s';

DROP TABLE host;
