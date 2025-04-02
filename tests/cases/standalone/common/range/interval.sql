CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
);

INSERT INTO TABLE host VALUES
    ("1970-01-01T01:00:00+00:00", 'host1', 0),
    ("1970-01-01T02:00:00+00:00", 'host1', 1),
    ("1971-01-02T03:00:00+00:00", 'host1', 2),
    ("1971-01-02T04:00:00+00:00", 'host1', 3),
    ("1970-01-01T01:00:00+00:00", 'host2', 4),
    ("1970-01-01T02:00:00+00:00", 'host2', 5),
    ("1971-01-02T03:00:00+00:00", 'host2', 6),
    ("1971-01-02T04:00:00+00:00", 'host2', 7);

SELECT ts, host, min(val) RANGE (INTERVAL '1 year') FROM host ALIGN (INTERVAL '1 year') ORDER BY host, ts;

SELECT ts, host, min(val) RANGE (INTERVAL '1' day) FROM host ALIGN (INTERVAL '1' day) ORDER BY host, ts;

SELECT ts, host, min(val) RANGE ('1 day'::INTERVAL) FROM host ALIGN ('1 day'::INTERVAL) ORDER BY host, ts;

SELECT ts, host, min(val) RANGE ('1 hour'::INTERVAL) FROM host ALIGN ('1 hour'::INTERVAL) ORDER BY host, ts;

SELECT ts, host, min(val) RANGE ('30 minute'::INTERVAL + '30 minute'::INTERVAL) FROM host ALIGN ('30 minute'::INTERVAL + '30 minute'::INTERVAL) ORDER BY host, ts;

DROP TABLE host;
