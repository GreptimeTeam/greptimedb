CREATE TABLE t_inverted_index_survival (
  host STRING INVERTED INDEX,
  msg STRING INVERTED INDEX,
  msg2 STRING,
  ts TIMESTAMP TIME INDEX,
  cpu DOUBLE,
  PRIMARY KEY(host)
) ENGINE=mito;

INSERT INTO t_inverted_index_survival VALUES
  ('h1', 'first event', 'first note', '2026-07-07 00:00:00+0000', 1.0),
  ('h2', 'second event', 'second note', '2026-07-07 00:01:00+0000', 2.0);

ADMIN FLUSH_TABLE('t_inverted_index_survival');