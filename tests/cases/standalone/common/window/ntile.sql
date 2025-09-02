-- Migrated from DuckDB test: test/sql/window/test_ntile.test

CREATE TABLE "Scoreboard"("TeamName" VARCHAR, "Player" VARCHAR, "Score" INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO "Scoreboard" VALUES 
('Mongrels', 'Apu', 350, 1000),
('Mongrels', 'Ned', 666, 2000),
('Mongrels', 'Meg', 1030, 3000),
('Mongrels', 'Burns', 1270, 4000),
('Simpsons', 'Homer', 1, 5000),
('Simpsons', 'Lisa', 710, 6000),
('Simpsons', 'Marge', 990, 7000),
('Simpsons', 'Bart', 2010, 8000);

-- NTILE with partition
SELECT "TeamName", "Player", "Score", NTILE(2) OVER (PARTITION BY "TeamName" ORDER BY "Score" ASC) AS ntile_val
FROM "Scoreboard" ORDER BY "TeamName", "Score";

-- NTILE without partition
SELECT "TeamName", "Player", "Score", NTILE(2) OVER (ORDER BY "Score" ASC) AS ntile_val
FROM "Scoreboard" ORDER BY "Score";

-- NTILE with large number
SELECT "TeamName", "Score", NTILE(1000) OVER (PARTITION BY "TeamName" ORDER BY "Score" ASC) AS ntile_val
FROM "Scoreboard" ORDER BY "TeamName", "Score";

-- NTILE with 1 (all rows in same tile)
SELECT "TeamName", "Score", NTILE(1) OVER (PARTITION BY "TeamName" ORDER BY "Score" ASC) AS ntile_val
FROM "Scoreboard" ORDER BY "TeamName", "Score";

-- NTILE with NULL (should return NULL)
-- TODO: duckdb return null, but GreptimeDB raise an error
SELECT "TeamName", "Score", NTILE(NULL) OVER (PARTITION BY "TeamName" ORDER BY "Score" ASC) AS ntile_val
FROM "Scoreboard" ORDER BY "TeamName", "Score";

DROP TABLE "Scoreboard";