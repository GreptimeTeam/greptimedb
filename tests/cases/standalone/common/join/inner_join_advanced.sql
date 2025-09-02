-- Migrated from DuckDB test: test/sql/join/inner/ advanced tests
-- Tests advanced inner join patterns

CREATE TABLE customers(cust_id INTEGER, cust_name VARCHAR, city VARCHAR, ts TIMESTAMP TIME INDEX);
CREATE TABLE orders(order_id INTEGER, cust_id INTEGER, order_date DATE, amount DOUBLE, ts TIMESTAMP TIME INDEX);
CREATE TABLE order_items(item_id INTEGER, order_id INTEGER, product VARCHAR, quantity INTEGER, price DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO customers VALUES 
(1, 'John', 'NYC', 1000), (2, 'Jane', 'LA', 2000), (3, 'Bob', 'Chicago', 3000), (4, 'Alice', 'NYC', 4000);

INSERT INTO orders VALUES 
(101, 1, '2023-01-01', 250.00, 1000), (102, 2, '2023-01-02', 180.00, 2000),
(103, 1, '2023-01-03', 420.00, 3000), (104, 3, '2023-01-04', 95.00, 4000),
(105, 4, '2023-01-05', 310.00, 5000);

INSERT INTO order_items VALUES 
(1, 101, 'Widget', 2, 125.00, 1000), (2, 101, 'Gadget', 1, 0.00, 2000),
(3, 102, 'Tool', 3, 60.00, 3000), (4, 103, 'Device', 1, 420.00, 4000),
(5, 104, 'Part', 5, 19.00, 5000), (6, 105, 'Component', 2, 155.00, 6000);

-- Multi-table inner join
SELECT 
  c.cust_name, c.city, o.order_id, o.order_date, o.amount
FROM customers c
INNER JOIN orders o ON c.cust_id = o.cust_id
ORDER BY o.order_date, c.cust_name;

-- Three-way inner join
SELECT 
  c.cust_name, o.order_id, oi.product, oi.quantity, oi.price
FROM customers c
INNER JOIN orders o ON c.cust_id = o.cust_id  
INNER JOIN order_items oi ON o.order_id = oi.order_id
ORDER BY c.cust_name, o.order_id, oi.product;

-- Inner join with complex conditions
SELECT 
  c.cust_name, o.order_id, o.amount
FROM customers c
INNER JOIN orders o ON c.cust_id = o.cust_id AND o.amount > 200.00
ORDER BY o.amount DESC;

-- Inner join with aggregation
SELECT 
  c.city,
  COUNT(o.order_id) as total_orders,
  SUM(o.amount) as total_amount,
  AVG(o.amount) as avg_order_amount
FROM customers c
INNER JOIN orders o ON c.cust_id = o.cust_id
GROUP BY c.city
ORDER BY total_amount DESC;

-- Self join
SELECT 
  o1.order_id as order1, o2.order_id as order2, o1.amount, o2.amount
FROM orders o1
INNER JOIN orders o2 ON o1.cust_id = o2.cust_id AND o1.order_id < o2.order_id
ORDER BY o1.order_id, o2.order_id;

-- Join with subquery
SELECT 
  c.cust_name, high_orders.total_amount
FROM customers c
INNER JOIN (
  SELECT cust_id, SUM(amount) as total_amount
  FROM orders 
  GROUP BY cust_id
  HAVING SUM(amount) > 300
) high_orders ON c.cust_id = high_orders.cust_id
ORDER BY high_orders.total_amount DESC;

-- Join with window functions
SELECT 
  c.cust_name,
  o.order_id, 
  o.amount,
  ROW_NUMBER() OVER (PARTITION BY c.cust_id ORDER BY o.order_date) as order_sequence
FROM customers c
INNER JOIN orders o ON c.cust_id = o.cust_id
ORDER BY c.cust_name, order_sequence;

DROP TABLE customers;

DROP TABLE orders;

DROP TABLE order_items;