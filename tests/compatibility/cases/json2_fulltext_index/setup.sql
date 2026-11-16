CREATE TABLE logs (ts TIMESTAMP TIME INDEX, j JSON2(max_auto_expanded_paths=0)) WITH ('append_mode'='true');
INSERT INTO logs VALUES (1, '{"message":"error old"}');
ADMIN FLUSH_TABLE('logs');
