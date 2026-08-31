CREATE TABLE stddev_values (
  seq_id INT PRIMARY KEY,
  grp INT,
  val DOUBLE,
  ts TIMESTAMP TIME INDEX DEFAULT now()
);

INSERT INTO stddev_values (seq_id, grp, val) VALUES
  (1, 0, 1.0),
  (2, 0, 2.0),
  (3, 1, 3.0),
  (4, 1, 4.0);

CREATE TABLE stddev_states (
  grp INT PRIMARY KEY,
  state BINARY,
  ts TIMESTAMP TIME INDEX DEFAULT now()
);

INSERT INTO stddev_states (grp, state)
SELECT grp, stddev_pop_state(val)
FROM stddev_values
GROUP BY grp;

ADMIN FLUSH_TABLE('stddev_values');
ADMIN FLUSH_TABLE('stddev_states');
