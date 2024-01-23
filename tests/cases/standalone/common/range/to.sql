CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
);

INSERT INTO TABLE host VALUES
    ("2024-01-23T22:30:00+00:00", 'host1', 0),
    ("2024-01-23T23:30:00+00:00", 'host1', 1),
    ("2024-01-24T22:30:00+00:00", 'host1', 2),
    ("2024-01-24T23:30:00+00:00", 'host1', 3),
    ("2024-01-23T22:30:00+00:00", 'host2', 4),
    ("2024-01-23T23:30:00+00:00", 'host2', 5),
    ("2024-01-24T22:30:00+00:00", 'host2', 6),
    ("2024-01-24T23:30:00+00:00", 'host2', 7);

SELECT ts, host, min(val) RANGE '1d' FROM host ALIGN '1d' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '1d' FROM host ALIGN '1d' TO UNKNOWN ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '1d' FROM host ALIGN '1d' TO '1900-01-01T00:00:00+01:00' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '1d' FROM host ALIGN '1d' TO '2024-01-23T00:00:00+01:00' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '1d' FROM host ALIGN '1d' TO '2023-01-01T00:00:00+01:00' ORDER BY host, ts;

SELECT ts, min(val) RANGE (INTERVAL '1' day) FROM host ALIGN (INTERVAL '1' day) TO '1900-01-01T00:00:00+01:00' by (1) ORDER BY ts;

--- ALIGN TO with time zone ---
set time_zone='Asia/Shanghai';

---- align to 'Asia/Shanghai' unix epoch 0 ----
SELECT ts, host, min(val) RANGE '1d' FROM host ALIGN '1d' ORDER BY host, ts;

set time_zone='UTC';

DROP TABLE host;
