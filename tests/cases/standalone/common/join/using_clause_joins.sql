-- Migrated from DuckDB test: test/sql/join/ USING clause tests
-- Tests USING clause join syntax

CREATE TABLE orders_using(order_id INTEGER, customer_id INTEGER, total DOUBLE, ts TIMESTAMP TIME INDEX);

CREATE TABLE customers_using(customer_id INTEGER, customer_name VARCHAR, city VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE payments_using(payment_id INTEGER, order_id INTEGER, amount DOUBLE, "method" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO orders_using VALUES
(1, 101, 250.00, 1000), (2, 102, 180.00, 2000), (3, 101, 420.00, 3000), (4, 103, 95.00, 4000);

INSERT INTO customers_using VALUES
(101, 'John Doe', 'NYC', 1000), (102, 'Jane Smith', 'LA', 2000), (103, 'Bob Wilson', 'Chicago', 3000);

INSERT INTO payments_using VALUES
(1, 1, 250.00, 'Credit', 1000), (2, 2, 180.00, 'Debit', 2000),
(3, 3, 420.00, 'Credit', 3000), (4, 4, 95.00, 'Cash', 4000);

-- Basic USING clause join
SELECT order_id, customer_name, total
FROM orders_using
INNER JOIN customers_using USING (customer_id)
ORDER BY order_id;

-- Multiple USING clause joins
SELECT o.order_id, c.customer_name, p.amount, p."method"
FROM orders_using o
INNER JOIN customers_using c USING (customer_id)
INNER JOIN payments_using p USING (order_id)
ORDER BY o.order_id;

-- LEFT JOIN with USING
SELECT c.customer_name, o.order_id, o.total
FROM customers_using c
LEFT JOIN orders_using o USING (customer_id)
ORDER BY c.customer_id, o.order_id;

-- USING with aggregation
SELECT
  c.city,
  COUNT(o.order_id) as order_count,
  SUM(o.total) as total_sales
FROM customers_using c
LEFT JOIN orders_using o USING (customer_id)
GROUP BY c.city
ORDER BY total_sales DESC;

-- USING with complex expressions
SELECT
  c.customer_name,
  COUNT(o.order_id) as order_count,
  COALESCE(SUM(o.total), 0) as total_spent
FROM customers_using c
LEFT JOIN orders_using o USING (customer_id)
GROUP BY c.customer_id, c.customer_name
HAVING COUNT(o.order_id) != 0
ORDER BY total_spent DESC;

DROP TABLE orders_using;

DROP TABLE customers_using;

DROP TABLE payments_using;
