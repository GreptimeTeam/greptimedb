-- Tests join edge cases and special scenarios

CREATE TABLE empty_table("id" INTEGER, "value" VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE single_row("id" INTEGER, "data" VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE duplicate_keys("id" INTEGER, "description" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO single_row VALUES (1, 'only_row', 1000);

INSERT INTO duplicate_keys VALUES (1, 'first', 1000), (1, 'second', 2000), (2, 'third', 3000), (2, 'fourth', 4000);

-- Join with empty table
SELECT s."id", s."data", e."value" FROM single_row s LEFT JOIN empty_table e ON s."id" = e."id" ORDER BY s."id";

-- Join with duplicate keys
SELECT s."id", s."data", d."description" FROM single_row s LEFT JOIN duplicate_keys d ON s."id" = d."id" ORDER BY d."description";

-- Self-join with duplicates
SELECT d1."description" as desc1, d2."description" as desc2 FROM duplicate_keys d1 INNER JOIN duplicate_keys d2 ON d1."id" = d2."id" AND d1.ts < d2.ts ORDER BY d1.ts, d2.ts;

DROP TABLE empty_table;

DROP TABLE single_row;

DROP TABLE duplicate_keys;
