CREATE TABLE uddsketch_values (
  seq_id INT PRIMARY KEY,
  val DOUBLE,
  ts TIMESTAMP TIME INDEX DEFAULT now()
);

INSERT INTO uddsketch_values (seq_id, val) VALUES
  (1, 10.0),
  (2, 20.0),
  (3, 30.0),
  (4, 40.0),
  (5, 50.0),
  (6, 60.0),
  (7, 70.0),
  (8, 80.0),
  (9, 90.0),
  (10, 100.0);

CREATE TABLE uddsketch_states (
  state BINARY,
  grp INT PRIMARY KEY,
  ts TIMESTAMP TIME INDEX DEFAULT now()
);

INSERT INTO uddsketch_states (state, grp)
SELECT uddsketch_state(128, 0.01, val), seq_id / 5 * 5 AS grp
FROM uddsketch_values
GROUP BY grp;

ADMIN FLUSH_TABLE('uddsketch_values');
ADMIN FLUSH_TABLE('uddsketch_states');
