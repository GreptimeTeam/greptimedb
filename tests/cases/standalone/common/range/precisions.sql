CREATE TABLE host_sec (
  ts timestamp(0) time index,
  host STRING PRIMARY KEY,
  val DOUBLE,
);

INSERT INTO TABLE host_sec VALUES
    (0,  'host1', 0),
    (5,  'host1', null),
    (10, 'host1', 1),
    (15, 'host1', null),
    (20, 'host1', 2),
    (0,  'host2', 3),
    (5,  'host2', null),
    (10, 'host2', 4),
    (15, 'host2', null),
    (20, 'host2', 5);

-- Test on Timestamps of different precisions

SELECT ts, host, min(val) RANGE '5s' FROM host_sec ALIGN '5s' ORDER BY host, ts;

DROP TABLE host_sec;
