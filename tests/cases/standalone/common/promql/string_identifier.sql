CREATE TABLE promql_string_identifier (
  ts timestamp(3) time index,
  "service.name" STRING,
  host STRING,
  val BIGINT,
  PRIMARY KEY("service.name", host),
);

INSERT INTO TABLE promql_string_identifier VALUES
    (0, 'api-server', 'h1', 1),
    (5000, 'db', 'host2', 2);

-- string identifier for label names with dots
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') promql_string_identifier{"service.name"="api-server"};

-- string identifier for metric names in label matchers
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') {"promql_string_identifier"};

-- string identifier in grouping labels
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') sum by ("service.name") (promql_string_identifier);

-- escaped hex in label matcher value
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') promql_string_identifier{host="\x68\x31"};

DROP TABLE promql_string_identifier;
