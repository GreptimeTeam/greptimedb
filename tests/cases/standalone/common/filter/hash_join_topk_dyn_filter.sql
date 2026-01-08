-- Dynamic Filter Test: Combined Hash Join + TopK
-- Tests dynamic filters with both hash join and TopK operators

-- Test 3.1: Hash Join with TopK Subquery
CREATE TABLE orders ("id" INT, customer_id INT, amount DOUBLE, ts TIMESTAMP TIME INDEX);
CREATE TABLE customers (customer_id INT, "name" STRING, tier STRING, ts TIMESTAMP TIME INDEX);

-- Insert test data
INSERT INTO orders VALUES
  (1, 1, 100.0, '2024-01-01 00:00:00'),
  (2, 2, 200.0, '2024-01-02 00:00:00'),
  (3, 3, 300.0, '2024-01-03 00:00:00'),
  (4, 1, 150.0, '2024-01-04 00:00:00'),
  (5, 2, 250.0, '2024-01-05 00:00:00'),
  (6, 1, 175.0, '2024-01-06 00:00:00'),
  (7, 3, 325.0, '2024-01-07 00:00:00'),
  (8, 2, 225.0, '2024-01-08 00:00:00'),
  (9, 1, 400.0, '2024-01-09 00:00:00'),
  (10, 3, 500.0, '2024-01-10 00:00:00');

INSERT INTO customers VALUES
  (1, 'Alice', 'gold', '2024-01-01 00:00:00'),
  (2, 'Bob', 'silver', '2024-01-02 00:00:00'),
  (3, 'Charlie', 'bronze', '2024-01-03 00:00:00');

-- Test: Hash join where probe side uses TopK
-- This combines both dynamic filter sources: TopK generates filter on amount,
-- and hash join generates filter on customer_id
EXPLAIN SELECT top_orders."id", top_orders.amount, c."name", c.tier
FROM (
  SELECT "id", customer_id, amount, ts
  FROM orders
  ORDER BY amount DESC
  LIMIT 5
) top_orders
JOIN customers c ON top_orders.customer_id = c.customer_id
WHERE c.tier IN ('gold', 'bronze');

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
EXPLAIN ANALYZE SELECT top_orders."id", top_orders.amount, c."name", c.tier
FROM (
  SELECT "id", customer_id, amount, ts
  FROM orders
  ORDER BY amount DESC
  LIMIT 5
) top_orders
JOIN customers c ON top_orders.customer_id = c.customer_id
WHERE c.tier IN ('gold', 'bronze');

-- Verify correctness
-- SQLNESS SORT_RESULT
SELECT top_orders."id", top_orders.amount, c."name", c.tier
FROM (
  SELECT "id", customer_id, amount, ts
  FROM orders
  ORDER BY amount DESC
  LIMIT 5
) top_orders
JOIN customers c ON top_orders.customer_id = c.customer_id
WHERE c.tier IN ('gold', 'bronze');

DROP TABLE orders;
DROP TABLE customers;

-- Test 3.2: TopK with Hash Join Subquery
CREATE TABLE orders ("id" INT, customer_id INT, amount DOUBLE, ts TIMESTAMP TIME INDEX);
CREATE TABLE customers (customer_id INT, "name" STRING, tier STRING, ts TIMESTAMP TIME INDEX);

-- Insert test data
INSERT INTO orders VALUES
  (1, 1, 100.0, '2024-01-01 00:00:00'),
  (2, 2, 200.0, '2024-01-02 00:00:00'),
  (3, 3, 300.0, '2024-01-03 00:00:00'),
  (4, 1, 150.0, '2024-01-04 00:00:00'),
  (5, 2, 250.0, '2024-01-05 00:00:00'),
  (6, 1, 175.0, '2024-01-06 00:00:00'),
  (7, 3, 325.0, '2024-01-07 00:00:00'),
  (8, 2, 225.0, '2024-01-08 00:00:00'),
  (9, 1, 400.0, '2024-01-09 00:00:00'),
  (10, 3, 500.0, '2024-01-10 00:00:00');

INSERT INTO customers VALUES
  (1, 'Alice', 'gold', '2024-01-01 00:00:00'),
  (2, 'Bob', 'silver', '2024-01-02 00:00:00'),
  (3, 'Charlie', 'bronze', '2024-01-03 00:00:00');

-- Test: TopK where sorting side uses Hash Join
-- Hash join produces customer info, then TopK sorts the joined results
-- This tests that TopK dynamic filter works on hash join output
EXPLAIN SELECT "id", customer_id, "name", tier, amount
FROM (
  SELECT o."id", o.customer_id, c."name", c.tier, o.amount
  FROM orders o
  JOIN customers c ON o.customer_id = c.customer_id
  WHERE c.tier IN ('gold', 'silver')
)
ORDER BY amount DESC
LIMIT 4;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
EXPLAIN ANALYZE SELECT "id", customer_id, "name", tier, amount
FROM (
  SELECT o."id", o.customer_id, c."name", c.tier, o.amount
  FROM orders o
  JOIN customers c ON o.customer_id = c.customer_id
  WHERE c.tier IN ('gold', 'silver')
)
ORDER BY amount DESC
LIMIT 4;

-- Verify correctness
SELECT "id", customer_id, "name", tier, amount
FROM (
  SELECT o."id", o.customer_id, c."name", c.tier, o.amount
  FROM orders o
  JOIN customers c ON o.customer_id = c.customer_id
  WHERE c.tier IN ('gold', 'silver')
)
ORDER BY amount DESC
LIMIT 4;

DROP TABLE orders;
DROP TABLE customers;

-- Test 3.3: Multiple Dynamic Filters
CREATE TABLE orders ("id" INT, customer_id INT, product_id INT, amount DOUBLE, ts TIMESTAMP TIME INDEX);
CREATE TABLE customers (customer_id INT, "name" STRING, tier STRING, ts TIMESTAMP TIME INDEX);
CREATE TABLE products (product_id INT, "name" STRING, category STRING, ts TIMESTAMP TIME INDEX);

-- Insert test data
INSERT INTO orders VALUES
  (1, 1, 101, 100.0, '2024-01-01 00:00:00'),
  (2, 2, 102, 200.0, '2024-01-02 00:00:00'),
  (3, 3, 103, 300.0, '2024-01-03 00:00:00'),
  (4, 1, 101, 150.0, '2024-01-04 00:00:00'),
  (5, 2, 102, 250.0, '2024-01-05 00:00:00'),
  (6, 1, 103, 175.0, '2024-01-06 00:00:00'),
  (7, 3, 101, 325.0, '2024-01-07 00:00:00'),
  (8, 2, 102, 225.0, '2024-01-08 00:00:00'),
  (9, 1, 103, 400.0, '2024-01-09 00:00:00'),
  (10, 3, 101, 500.0, '2024-01-10 00:00:00');

INSERT INTO customers VALUES
  (1, 'Alice', 'gold', '2024-01-01 00:00:00'),
  (2, 'Bob', 'silver', '2024-01-02 00:00:00'),
  (3, 'Charlie', 'bronze', '2024-01-03 00:00:00');

INSERT INTO products VALUES
  (101, 'Laptop', 'electronics', '2024-01-01 00:00:00'),
  (102, 'Mouse', 'electronics', '2024-01-02 00:00:00'),
  (103, 'Keyboard', 'electronics', '2024-01-03 00:00:00');

-- Test: Multiple hash joins producing dynamic filters
-- This creates a chain: orders JOIN customers JOIN products
-- Both hash joins can produce dynamic filters
EXPLAIN SELECT o."id", o.amount, c."name", c.tier, p."name" as product_name, p.category
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE c.tier IN ('gold', 'silver')
  AND p.category = 'electronics';

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
EXPLAIN ANALYZE SELECT o."id", o.amount, c."name", c.tier, p."name" as product_name, p.category
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE c.tier IN ('gold', 'silver')
  AND p.category = 'electronics';

-- Verify correctness
-- SQLNESS SORT_RESULT
SELECT o."id", o.amount, c."name", c.tier, p."name" as product_name, p.category
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE c.tier IN ('gold', 'silver')
  AND p.category = 'electronics';

DROP TABLE orders;
DROP TABLE customers;
DROP TABLE products;
