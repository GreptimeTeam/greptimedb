select timestamptz '2021-11-15 02:30:00';

select '2021-11-15 02:30:00'::TIMESTAMP::TIMESTAMPTZ;

SELECT '2021-04-29 10:50:09-05'::TIMESTAMPTZ::DATE;

SELECT '2021-04-29 10:50:09-05'::TIMESTAMPTZ::TIME;

CREATE TABLE dictionary_output (
    label STRING PRIMARY KEY,
    epoch_seconds BIGINT,
    ts TIMESTAMP TIME INDEX
);

INSERT INTO dictionary_output VALUES ('a', 42, 0), ('b', NULL, 1000);

SELECT from_unixtime(epoch_seconds, 'Europe/Paris') AS instant
FROM dictionary_output ORDER BY ts;

SELECT label, from_unixtime(epoch_seconds, 'Europe/Paris') AS instant
FROM dictionary_output ORDER BY ts;

-- SQLNESS PROTOCOL MYSQL
SELECT label, from_unixtime(epoch_seconds, 'Europe/Paris') AS instant
FROM dictionary_output ORDER BY ts;

DROP TABLE dictionary_output;
