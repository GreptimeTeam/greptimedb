-- description: Test unicode strings

-- insert unicode strings into the database
CREATE TABLE emojis(id INTEGER, s VARCHAR, ts timestamp time index);

INSERT INTO emojis VALUES (1, '🦆', 1), (2, '🦆🍞🦆', 2);

-- retrieve unicode strings again
SELECT * FROM emojis ORDER BY id;

-- substr on unicode
SELECT substr(s, 1, 1), substr(s, 2, 1) FROM emojis ORDER BY id;

SELECT substr('u🦆', 1, 1);

SELECT substr('A3🦤u🦆f', 4, 3);

SELECT substr('🦤🦆f', 1, 2);

-- length on emojis
SELECT length(s) FROM emojis ORDER BY id;

DROP TABLE emojis;
