CREATE TABLE t (ts TIMESTAMP TIME INDEX, j JSON(format = "structured") DEFAULT '{"foo": "bar"}');

CREATE TABLE t (ts TIMESTAMP TIME INDEX, j JSON(format = "structured"));

DESC TABLE t;

INSERT INTO t VALUES
(1762128001000, '{"int": 1}'),
(1762128002000, '{"int": 2, "list": [0.1, 0.2, 0.3]}'),
(1762128003000, '{"int": 3, "list": [0.4, 0.5, 0.6], "nested": {"a": {"x": "hello"}, "b": {"y": -1}}}');

DESC TABLE t;

INSERT INTO t VALUES
(1762128004000, '{"int": 4, "bool": true, "nested": {"a": {"y": 1}}}'),
(1762128005000, '{"int": 5, "bool": false, "nested": {"b": {"x": "world"}}}');

DESC TABLE t;

INSERT INTO t VALUES (1762128006000, '{"int": 6, "list": [-6.0], "bool": true, "nested": {"a": {"x": "ax", "y": 66}, "b": {"y": -66, "x": "bx"}}}');

DESC TABLE t;

INSERT INTO t VALUES (1762128011000, '{}');

SELECT ts, j FROM t order by ts;

DROP table t;
