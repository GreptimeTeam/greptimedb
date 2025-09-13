-- Tests join result ordering and deterministic behavior

CREATE TABLE left_data("id" INTEGER, left_val VARCHAR, sort_key INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE right_data("id" INTEGER, right_val VARCHAR, sort_key INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO left_data VALUES (1, 'A1', 10, 1000), (2, 'A2', 5, 2000), (3, 'A3', 15, 3000);

INSERT INTO right_data VALUES (1, 'B1', 20, 1000), (2, 'B2', 8, 2000), (4, 'B4', 12, 3000);

SELECT l."id", l.left_val, r.right_val, l.sort_key + r.sort_key as combined_sort
FROM left_data l INNER JOIN right_data r ON l."id" = r."id" ORDER BY l."id";

SELECT l.left_val, r.right_val FROM left_data l FULL OUTER JOIN right_data r ON l."id" = r."id" ORDER BY l."id", r."id";

DROP TABLE left_data;

DROP TABLE right_data;
