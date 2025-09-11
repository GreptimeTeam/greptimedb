-- Migrated from DuckDB test: test/sql/join/ subquery join tests
-- Tests joins involving subqueries

CREATE TABLE products_sub(prod_id INTEGER, prod_name VARCHAR, category_id INTEGER, price DOUBLE, ts TIMESTAMP TIME INDEX);

CREATE TABLE categories_sub(cat_id INTEGER, cat_name VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE sales_sub(sale_id INTEGER, prod_id INTEGER, quantity INTEGER, sale_date DATE, ts TIMESTAMP TIME INDEX);

INSERT INTO products_sub VALUES
(1, 'Laptop', 1, 1200.00, 1000), (2, 'Mouse', 2, 25.00, 2000),
(3, 'Monitor', 1, 400.00, 3000), (4, 'Keyboard', 2, 80.00, 4000),
(5, 'Tablet', 1, 600.00, 5000);

INSERT INTO categories_sub VALUES
(1, 'Electronics', 1000), (2, 'Accessories', 2000);

INSERT INTO sales_sub VALUES
(1, 1, 2, '2023-01-01', 1000), (2, 2, 10, '2023-01-02', 2000),
(3, 3, 1, '2023-01-03', 3000), (4, 1, 1, '2023-01-04', 4000),
(5, 4, 5, '2023-01-05', 5000), (6, 5, 2, '2023-01-06', 6000);

-- Join with aggregated subquery
SELECT
  p.prod_name, p.price, sales_summary.total_quantity, sales_summary.total_sales
FROM products_sub p
INNER JOIN (
  SELECT prod_id, SUM(quantity) as total_quantity, COUNT(*) as total_sales
  FROM sales_sub
  GROUP BY prod_id
) sales_summary ON p.prod_id = sales_summary.prod_id
ORDER BY sales_summary.total_quantity DESC;

-- Join with filtered subquery
SELECT
  p.prod_name, c.cat_name, high_sales.quantity
FROM products_sub p
INNER JOIN categories_sub c ON p.category_id = c.cat_id
INNER JOIN (
  SELECT prod_id, quantity
  FROM sales_sub
  WHERE quantity > 3
) high_sales ON p.prod_id = high_sales.prod_id
ORDER BY high_sales.quantity DESC;

-- Join with correlated subquery results
SELECT
  p.prod_name, p.price,
  (SELECT SUM(s.quantity) FROM sales_sub s WHERE s.prod_id = p.prod_id) as total_sold
FROM products_sub p
WHERE EXISTS (SELECT 1 FROM sales_sub s WHERE s.prod_id = p.prod_id)
ORDER BY total_sold DESC;

-- Join subquery with window functions
SELECT
  p.prod_name, ranked_sales.quantity, ranked_sales.sale_rank
FROM products_sub p
INNER JOIN (
  SELECT
    prod_id, quantity,
    ROW_NUMBER() OVER (PARTITION BY prod_id ORDER BY quantity DESC) as sale_rank
  FROM sales_sub
) ranked_sales ON p.prod_id = ranked_sales.prod_id
WHERE ranked_sales.sale_rank = 1
ORDER BY ranked_sales.quantity DESC, p.prod_name ASC;

-- Multiple subquery joins
SELECT
  product_stats.prod_name,
  product_stats.avg_price,
  category_stats.category_sales
FROM (
  SELECT prod_id, prod_name, AVG(price) as avg_price, category_id
  FROM products_sub
  GROUP BY prod_id, prod_name, category_id
) product_stats
INNER JOIN (
  SELECT c.cat_id, c.cat_name, COUNT(s.sale_id) as category_sales
  FROM categories_sub c
  INNER JOIN products_sub p ON c.cat_id = p.category_id
  INNER JOIN sales_sub s ON p.prod_id = s.prod_id
  GROUP BY c.cat_id, c.cat_name
) category_stats ON product_stats.category_id = category_stats.cat_id
ORDER BY category_stats.category_sales DESC, product_stats.prod_name DESC;

DROP TABLE products_sub;

DROP TABLE categories_sub;

DROP TABLE sales_sub;
