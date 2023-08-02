CREATE TABLE blobs (b BYTEA, t timestamp time index);

--Insert valid hex strings--
INSERT INTO blobs VALUES('\xaa\xff\xaa'::BYTEA, 1), ('\xAA\xFF\xAA\xAA\xFF\xAA'::BYTEA, 2), ('\xAA\xFF\xAA\xAA\xFF\xAA\xAA\xFF\xAA'::BYTEA, 3);

SELECT * FROM blobs;

--Insert valid hex strings, lower case--
DELETE FROM blobs;

INSERT INTO blobs VALUES('\xaa\xff\xaa'::BYTEA, 1), ('\xaa\xff\xaa\xaa\xff\xaa'::BYTEA, 2), ('\xaa\xff\xaa\xaa\xff\xaa\xaa\xff\xaa'::BYTEA, 3);

SELECT * FROM blobs;

--Insert valid hex strings with number and letters--
DELETE FROM blobs;

INSERT INTO blobs VALUES('\xaa1199'::BYTEA, 1), ('\xaa1199aa1199'::BYTEA, 2), ('\xaa1199aa1199aa1199'::BYTEA, 3);

SELECT * FROM blobs;

--Insert invalid hex strings (invalid hex chars: G, H, I)--
INSERT INTO blobs VALUES('\xGA\xFF\xAA'::BYTEA, 4);

--Insert invalid hex strings (odd # of chars)--
INSERT INTO blobs VALUES('\xA'::BYTEA, 4);

INSERT INTO blobs VALUES('\xAA\xA'::BYTEA, 4);

INSERT INTO blobs VALUES('blablabla\x'::BYTEA, 4);

SELECT 'abc �'::BYTEA;

SELECT ''::BYTEA;

SELECT NULL::BYTEA;

CREATE TABLE blob_empty (b BYTEA, t timestamp time index);

INSERT INTO blob_empty VALUES(''::BYTEA, 1), (''::BYTEA, 2);

INSERT INTO blob_empty VALUES(NULL, 3), (NULL::BYTEA, 4);

SELECT * FROM blob_empty;

SELECT '\x7F'::BYTEA;

drop table blobs;

drop table blob_empty;
