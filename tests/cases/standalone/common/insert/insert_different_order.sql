CREATE TABLE different_order(k0 STRING, k1 STRING, v0 INTEGER, v1 INTEGER, t TIMESTAMP, time index(t), primary key(k0, k1));

INSERT INTO different_order (v1, k1, k0, t, v0) VALUES (11, 'b0', 'a0', 1, 1);

INSERT INTO different_order (v1, v0, k0, t) VALUES (12, 2, 'a1', 2);

INSERT INTO different_order (t, v1, k0, k1) VALUES (3, 13, 'a2', 'b1');

INSERT INTO different_order (t, k0, k1) VALUES (4, 'a2', 'b1');

SELECT * from different_order order by t;

SELECT * from different_order WHERE k0 = 'a2' order by t;

DROP TABLE different_order;
