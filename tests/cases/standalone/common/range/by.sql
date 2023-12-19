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

-- Test by calculate

SELECT ts, length(host), max(val) RANGE '5s' FROM host ALIGN '20s' BY (length(host)) ORDER BY ts;

SELECT ts, max(val) RANGE '5s' FROM host ALIGN '20s' BY (2) ORDER BY ts;

-- The user explicitly specifies that the aggregation key is empty. In this case, there is no aggregation key. All data will be aggregated into a group.
-- Implement by rewrite `BY()` to `BY(1)` automatically through sqlparser. They are semantically equivalent.
SELECT ts, max(val) RANGE '5s' FROM host ALIGN '20s' BY () ORDER BY ts;

SELECT ts, length(host)::INT64 + 2, max(val) RANGE '5s' FROM host ALIGN '20s' BY (length(host)::INT64 + 2) ORDER BY ts;

-- Test error

-- project non-aggregation key
SELECT ts, host, max(val) RANGE '5s' FROM host ALIGN '20s' BY () ORDER BY ts;

DROP TABLE host;

-- Test no primary key and by keyword

CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING,
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

SELECT ts, max(val) RANGE '5s' FROM host ALIGN '20s' ORDER BY ts;

DROP TABLE host;
