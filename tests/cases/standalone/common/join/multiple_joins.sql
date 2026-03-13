-- Migrated from DuckDB test: Multiple join scenarios
-- Tests multiple table joins

CREATE TABLE customers_multi("id" INTEGER, "name" VARCHAR, city VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE orders_multi("id" INTEGER, customer_id INTEGER, product_id INTEGER, amount DOUBLE, ts TIMESTAMP TIME INDEX);

CREATE TABLE products_multi("id" INTEGER, "name" VARCHAR, "category" VARCHAR, price DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO customers_multi VALUES (1, 'Alice', 'NYC', 1000), (2, 'Bob', 'LA', 2000), (3, 'Carol', 'Chicago', 3000);

INSERT INTO orders_multi VALUES (101, 1, 1001, 150.0, 4000), (102, 2, 1002, 200.0, 5000), (103, 1, 1001, 150.0, 6000);

INSERT INTO products_multi VALUES (1001, 'Laptop', 'Electronics', 1000.0, 7000), (1002, 'Book', 'Education', 25.0, 8000);

-- Three table join
SELECT c."name", p."name" as product, o.amount
FROM customers_multi c
JOIN orders_multi o ON c."id" = o.customer_id
JOIN products_multi p ON o.product_id = p."id"
ORDER BY c."name", p."name";

-- Multiple joins with aggregation
SELECT
  c.city,
  p.category,
  COUNT(*) as order_count,
  SUM(o.amount) as total_amount
FROM customers_multi c
JOIN orders_multi o ON c."id" = o.customer_id
JOIN products_multi p ON o.product_id = p."id"
GROUP BY c.city, p.category
ORDER BY c.city, p.category;

-- Mixed join types
SELECT c."name", p."name" as product, o.amount
FROM customers_multi c
LEFT JOIN orders_multi o ON c."id" = o.customer_id
INNER JOIN products_multi p ON o.product_id = p."id"
ORDER BY c."name", p."name" NULLS LAST;

DROP TABLE products_multi;

DROP TABLE orders_multi;

DROP TABLE customers_multi;
