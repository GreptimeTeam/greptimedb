CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
);

INSERT INTO TABLE host VALUES
    ("1970-01-01T22:30:00+00:00", 'host1', 0),
    ("1970-01-01T23:30:00+00:00", 'host1', 1),
    ("1970-01-02T22:30:00+00:00", 'host1', 2),
    ("1970-01-02T23:30:00+00:00", 'host1', 3),
    ("1970-01-01T22:30:00+00:00", 'host2', 4),
    ("1970-01-01T23:30:00+00:00", 'host2', 5),
    ("1970-01-02T22:30:00+00:00", 'host2', 6),
    ("1970-01-02T23:30:00+00:00", 'host2', 7);

SELECT ts, host, min(val) RANGE '1d' FROM host ALIGN '1d' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '1d' FROM host ALIGN '1d' TO UNKNOWN ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '1d' FROM host ALIGN '1d' TO '1900-01-01T00:00:00+01:00' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '1d' FROM host ALIGN '1d' TO '1970-01-01T00:00:00+01:00' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '1d' FROM host ALIGN '1d' TO '2023-01-01T00:00:00+01:00' ORDER BY host, ts;

SELECT ts, min(val) RANGE (INTERVAL '1' day) FROM host ALIGN (INTERVAL '1' day) TO '1900-01-01T00:00:00+01:00' by (1) ORDER BY ts;

DROP TABLE host;
