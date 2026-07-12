CREATE TABLE IF NOT EXISTS demo_metric_table (
  label STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  val DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY (label)
)
PARTITION ON COLUMNS (label) (
  label < 'M',
  label >= 'M'
)
ENGINE=metric
WITH(
  physical_metric_table = 'true',
  skip_wal = 'true'
);

INSERT INTO demo_metric_table (label, ts, val)
VALUES ('A', '2026-05-19 00:00:00', 1.0);

DROP TABLE demo_metric_table;
