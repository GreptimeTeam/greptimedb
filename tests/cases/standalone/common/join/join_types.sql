-- Migrated from DuckDB test: test/sql/join/inner/test_join_types.test
-- Tests different join types and conditions

CREATE TABLE customers("id" INTEGER, "name" VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE orders(order_id INTEGER, customer_id INTEGER, amount INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO customers VALUES (1, 'Alice', 1000), (2, 'Bob', 2000), (3, 'Carol', 3000);

INSERT INTO orders VALUES (101, 1, 100, 4000), (102, 1, 200, 5000), (103, 2, 150, 6000), (104, 4, 300, 7000);

-- INNER JOIN
SELECT c."name", o.amount FROM customers c INNER JOIN orders o ON c."id" = o.customer_id ORDER BY c."name", o.amount;

-- LEFT JOIN showing unmatched customers
SELECT c."name", o.amount FROM customers c LEFT JOIN orders o ON c."id" = o.customer_id ORDER BY c."name", o.amount NULLS LAST;

-- RIGHT JOIN showing unmatched orders
SELECT c."name", o.amount FROM customers c RIGHT JOIN orders o ON c."id" = o.customer_id ORDER BY c."name" NULLS LAST, o.amount;

-- FULL OUTER JOIN showing all unmatched
SELECT c."name", o.amount FROM customers c FULL OUTER JOIN orders o ON c."id" = o.customer_id ORDER BY c."name" NULLS LAST, o.amount NULLS LAST;

-- JOIN with additional conditions
SELECT c."name", o.amount FROM customers c JOIN orders o ON c."id" = o.customer_id AND o.amount > 150 ORDER BY c."name";

DROP TABLE orders;

DROP TABLE customers;