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

CREATE VIEW tql_view_rfc3339 AS TQL EVAL ('1970-01-01T00:00:00Z', '1970-01-01T00:00:00Z', '1s', '5m') tql_view_source;

SELECT * FROM tql_view_rfc3339;

SHOW CREATE VIEW tql_view_rfc3339;

DROP VIEW tql_view_rfc3339;

DROP TABLE tql_view_source;
