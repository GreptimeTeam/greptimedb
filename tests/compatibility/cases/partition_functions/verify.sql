SHOW CREATE TABLE function_routes;
SELECT * FROM function_routes ORDER BY ts;
INSERT INTO function_routes VALUES ('abc', 1, 10), ('xyz', 2, 20), ('中🙂', 3, 30), (NULL, 4, 40);
SELECT * FROM function_routes ORDER BY ts;
SELECT * FROM function_routes WHERE host = 'abc';
DELETE FROM function_routes WHERE host = 'xyz';
SELECT * FROM function_routes ORDER BY ts;

SHOW CREATE TABLE integer_hash_routes;
SELECT * FROM integer_hash_routes ORDER BY ts;
INSERT INTO integer_hash_routes VALUES (42, 42, 'a', 1, 10), (-1, 18446744073709551615, 'b', 2, 20), (-9223372036854775808, 0, '中', 3, 30), (NULL, 42, 'a', 4, 40), (42, NULL, 'a', 5, 50), (42, 42, NULL, 6, 60);
SELECT * FROM integer_hash_routes ORDER BY ts;
DELETE FROM integer_hash_routes WHERE tenant_id = -1;
SELECT * FROM integer_hash_routes ORDER BY ts;
