CREATE TABLE tql_view_source(
  ts TIMESTAMP TIME INDEX,
  host STRING,
  val DOUBLE,
  PRIMARY KEY (host)
);

INSERT INTO tql_view_source VALUES (0, 'a', 2);

CREATE VIEW tql_view AS TQL EVAL (0, 0, '1s', '5m') tql_view_source;

SELECT * FROM tql_view;

SHOW CREATE VIEW tql_view;

DROP VIEW tql_view;

DROP TABLE tql_view_source;
