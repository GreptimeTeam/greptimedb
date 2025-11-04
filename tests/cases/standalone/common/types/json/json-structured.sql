CREATE TABLE t (ts TIMESTAMP TIME INDEX, j JSON(format = "structured"));

DESC TABLE t;

INSERT INTO t VALUES
(1762128000000, '{}'),
(1762128001000, '{"int": 1}'),
(1762128002000, '{"int": 2, "list": [-2.0]}'),
(1762128003000, '{"int": 3, "list": [-3.0], "nested": {"bool": false}}'),
(1762128004000, '{"int": 4, "list": [-4.0], "nested": {"string": "a"}}'),
(1762128005000, '{"int": 5, "list": [-5.0], "nested": {"string": "b", "bool": true}}');

INSERT INTO t VALUES (1762128006000, '{"int": -1, "list": [1.0], "nested": {"string": "c", "bool": false}}');

INSERT INTO t VALUES (1762128007000, '{"int": -2, "list": [2.0], "nested": {"string": "d"}}');

INSERT INTO t VALUES (1762128008000, '{"int": -3, "list": [3.0], "nested": {"bool": true}}');

INSERT INTO t VALUES (1762128009000, '{"int": -4, "list": [4.0]}');

INSERT INTO t VALUES (1762128010000, '{"int": -5}');

INSERT INTO t VALUES (1762128011000, '{}');

INSERT INTO t VALUES
(1762128012000, '{"int": 6, "list": [-6.0], "nested": {"string": "e", "bool": true}}'),
(1762128013000, '{"int": 7, "list": [-7.0], "nested": {"string": "f", "bool": true}}'),
(1762128014000, '{"int": 8, "list": [-8.0], "nested": {"string": "g", "bool": true}}');

SELECT ts, j FROM t;

DROP table t;
