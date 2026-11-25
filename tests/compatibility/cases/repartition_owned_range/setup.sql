CREATE TABLE repart_owned_range (
  host STRING,
  ts TIMESTAMP TIME INDEX,
  val INT,
  PRIMARY KEY (host)
) PARTITION ON COLUMNS (host) (
  host < 'h1',
  host >= 'h1' AND host < 'h2',
  host >= 'h2'
);

-- Rows straddle every partition boundary, including the exact bound values.
INSERT INTO repart_owned_range VALUES
  ('a', 1, 1),
  ('h1', 2, 2),
  ('h1n', 3, 3),
  ('h2', 4, 4),
  ('zz', 5, 5);

ADMIN FLUSH_TABLE('repart_owned_range');

-- Split the middle partition. The flushed SSTs are shared by the two new
-- references, each of which persists the narrower range it owns.
ALTER TABLE repart_owned_range REPARTITION (
  host >= 'h1' AND host < 'h2'
) INTO (
  host >= 'h1' AND host < 'h1m',
  host >= 'h1m' AND host < 'h2'
);

ADMIN FLUSH_TABLE('repart_owned_range');

INSERT INTO repart_owned_range VALUES ('h1m', 6, 6);
