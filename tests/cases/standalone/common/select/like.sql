CREATE TABLE host (
  ts TIMESTAMP(3) TIME INDEX,
  host STRING PRIMARY KEY,
  val DOUBLE,
);

INSERT INTO TABLE host VALUES
    (0, 'a+b', 1.0),
    (1, 'b+c', 2.0),
    (2, 'a', 3.0),
    (3, 'c', 4.0);

-- SQLNESS SORT_RESULT 3 1
SELECT * FROM host WHERE host LIKE '%+%';

DROP TABLE host;
