-- Migrated from DuckDB test: test/sql/join/ NULL handling tests
-- Tests join behavior with NULL values

CREATE TABLE table_with_nulls("id" INTEGER, "value" VARCHAR, "category" INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE lookup_table("category" INTEGER, cat_name VARCHAR, priority INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO table_with_nulls VALUES
(1, 'item1', 1, 1000), (2, 'item2', NULL, 2000), (3, 'item3', 2, 3000),
(4, 'item4', NULL, 4000), (5, 'item5', 3, 5000), (6, 'item6', 1, 6000);

INSERT INTO lookup_table VALUES
(1, 'Category A', 1, 1000), (2, 'Category B', 2, 2000), (3, 'Category C', 3, 3000), (NULL, 'Unknown', 0, 4000);

-- Inner join with NULLs (NULLs won't match)
SELECT t."id", t."value", l.cat_name
FROM table_with_nulls t
INNER JOIN lookup_table l ON t.category = l.category
ORDER BY t."id";

-- Left join with NULLs
SELECT t."id", t."value", t.category, COALESCE(l.cat_name, 'No Category') as category_name
FROM table_with_nulls t
LEFT JOIN lookup_table l ON t.category = l.category
ORDER BY t."id";

-- Join with explicit NULL handling
SELECT
  t."id", t."value",
  CASE
    WHEN t.category IS NULL THEN 'NULL Category'
    WHEN l.cat_name IS NULL THEN 'Missing Lookup'
    ELSE l.cat_name
  END as resolved_category
FROM table_with_nulls t
LEFT JOIN lookup_table l ON t.category = l.category
ORDER BY t."id";

-- NULL-safe join using COALESCE
SELECT t."id", t."value", l.cat_name
FROM table_with_nulls t
INNER JOIN lookup_table l ON COALESCE(t.category, -1) = COALESCE(l.category, -1)
ORDER BY t."id";

-- Aggregation with NULL join results
SELECT
  COALESCE(l.cat_name, 'Uncategorized') as category,
  COUNT(*) as item_count,
  COUNT(l.category) as matched_count,
  COUNT(*) - COUNT(l.category) as unmatched_count
FROM table_with_nulls t
LEFT JOIN lookup_table l ON t.category = l.category
GROUP BY l.cat_name
ORDER BY item_count DESC, category DESC;

DROP TABLE table_with_nulls;

DROP TABLE lookup_table;
