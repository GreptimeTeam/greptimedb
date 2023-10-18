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

SELECT ts, CAST(length(host) as INT64) + 2, max(val) RANGE '5s' FROM host ALIGN '20s' BY (CAST(length(host) as INT64) + 2) ORDER BY ts;

DROP TABLE host;
