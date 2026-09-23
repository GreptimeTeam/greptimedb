SHOW CREATE TABLE function_routes;
SELECT * FROM function_routes ORDER BY ts;
INSERT INTO function_routes VALUES ('abc', 1, 10), ('xyz', 2, 20), ('中🙂', 3, 30), (NULL, 4, 40);
SELECT * FROM function_routes ORDER BY ts;
SELECT * FROM function_routes WHERE host = 'abc';
DELETE FROM function_routes WHERE host = 'xyz';
SELECT * FROM function_routes ORDER BY ts;
