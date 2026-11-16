SELECT ts FROM logs WHERE matches_term(j.message, 'error') ORDER BY ts;
ALTER TABLE logs MODIFY COLUMN j JSON2(max_auto_expanded_paths=0, message STRING FULLTEXT INDEX WITH (granularity='1'));
INSERT INTO logs VALUES (2, '{"message":"error new"}');
ADMIN FLUSH_TABLE('logs');
SHOW CREATE TABLE logs;
SELECT ts FROM logs WHERE matches_term(j.message, 'error') ORDER BY ts;
-- SQLNESS ARG restart=true
SHOW CREATE TABLE logs;
SELECT ts FROM logs WHERE matches_term(j.message, 'error') ORDER BY ts;
