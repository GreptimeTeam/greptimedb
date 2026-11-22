CREATE TABLE blobs (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    payload BINARY STORAGE WITH (type = 'external', threshold = '64KiB'),
    body STRING STORAGE WITH (threshold = '1MiB') COMPRESSION WITH (level = 3),
    note STRING STORAGE WITH (type = 'EXTERNAL'),
);

SHOW CREATE TABLE blobs;

INSERT INTO blobs VALUES
    (1, 'h1', 'abc', 'first body', 'n1'),
    (2, 'h1', NULL, 'second body', NULL),
    (3, 'h2', 'xyz', NULL, 'n3');

ADMIN flush_table('blobs');

SELECT ts, host, payload, body, note FROM blobs ORDER BY ts;

ALTER TABLE blobs MODIFY COLUMN body BIGINT;

ALTER TABLE blobs ADD COLUMN extra STRING STORAGE WITH (threshold = '4KiB');

ALTER TABLE blobs ADD COLUMN region STRING PRIMARY KEY STORAGE WITH (threshold = '4KiB');

ALTER TABLE blobs ADD COLUMN n BIGINT STORAGE WITH (threshold = '4KiB');

CREATE TABLE blobs_like LIKE blobs;

SHOW CREATE TABLE blobs_like;

DROP TABLE blobs_like;

SHOW CREATE TABLE blobs;

DROP TABLE blobs;

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, s STRING STORAGE WITH (type = 'inline'));

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, s STRING STORAGE WITH (threshold = '0'));

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, s STRING STORAGE WITH (threshold = 'large'));

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, s STRING STORAGE WITH (size = '1MiB'));

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, s STRING STORAGE);

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, n BIGINT STORAGE WITH (type = 'external'));

CREATE TABLE bad (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY STORAGE WITH (type = 'external')
);

CREATE TABLE bad (ts TIMESTAMP TIME INDEX STORAGE WITH (type = 'external'));
