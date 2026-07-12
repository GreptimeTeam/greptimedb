-- description: Test unicode strings

-- insert unicode strings into the database
CREATE TABLE emojis(`id` INTEGER, s VARCHAR, ts timestamp time index);

INSERT INTO emojis VALUES (1, '🦆', 1), (2, '🦆🍞🦆', 2);

-- retrieve unicode strings again
SELECT * FROM emojis ORDER BY id;

-- substr on unicode
-- Because "substr" always use "Utf8View" as its return type, this query will return error
-- "column types must match schema types, expected Utf8View but found Utf8" if the "CAST" is not used.
-- TODO(LFC): support "Utf8View" Arrow array so that this "CAST" walkaround can be removed.
SELECT CAST(substr(s, 1, 1) AS STRING), CAST(substr(s, 2, 1) AS STRING) FROM emojis ORDER BY id;

SELECT substr('u🦆', 1, 1);

SELECT substr('A3🦤u🦆f', 4, 3);

SELECT substr('🦤🦆f', 1, 2);

-- length on emojis
SELECT length(s) FROM emojis ORDER BY id;

DROP TABLE emojis;
