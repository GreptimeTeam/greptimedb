CREATE TABLE projected_pk_group_by (
  marker TINYINT UNSIGNED,
  host STRING,
  ts TIMESTAMP TIME INDEX,
  PRIMARY KEY (marker, host)
);

INSERT INTO projected_pk_group_by VALUES
  (0, 'a', 1000),
  (0, 'a', 2000),
  (0, 'b', 1000),
  (0, 'b', 2000),
  (0, 'c', 1000),
  (1, NULL, 1000);

SELECT marker, count(*) AS row_count
FROM projected_pk_group_by
GROUP BY marker
ORDER BY marker;

SELECT marker, host, count(*) AS row_count
FROM projected_pk_group_by
GROUP BY marker, host
ORDER BY marker, host;

ADMIN FLUSH_TABLE('projected_pk_group_by');

SELECT marker, count(*) AS row_count
FROM projected_pk_group_by
GROUP BY marker
ORDER BY marker;

DROP TABLE projected_pk_group_by;
