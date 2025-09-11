-- Migrated from DuckDB test: test/sql/join/cross_product/ advanced tests
-- Tests advanced cross join scenarios

CREATE TABLE products(prod_id INTEGER, prod_name VARCHAR, price DOUBLE, ts TIMESTAMP TIME INDEX);

CREATE TABLE stores(store_id INTEGER, store_name VARCHAR, city VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE categories(cat_id INTEGER, cat_name VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO products VALUES
(1, 'Laptop', 999.99, 1000), (2, 'Mouse', 29.99, 2000), (3, 'Monitor', 299.99, 3000);

INSERT INTO stores VALUES
(1, 'TechStore', 'NYC', 1000), (2, 'GadgetShop', 'LA', 2000);

INSERT INTO categories VALUES
(1, 'Electronics', 1000), (2, 'Accessories', 2000);

-- Basic cross join
SELECT
  p.prod_name, s.store_name, s.city
FROM products p
CROSS JOIN stores s
ORDER BY p.prod_id, s.store_id;

-- Cross join with filtering
SELECT
  p.prod_name, s.store_name, p.price
FROM products p
CROSS JOIN stores s
WHERE p.price > 100.00
ORDER BY p.price DESC, s.store_name;

-- Triple cross join
SELECT
  p.prod_name, s.store_name, c.cat_name,
  CASE WHEN p.price > 500 THEN 'Premium' ELSE 'Standard' END as tier
FROM products p
CROSS JOIN stores s
CROSS JOIN categories c
ORDER BY p.prod_id, s.store_id, c.cat_id;

-- Cross join with aggregation
SELECT
  s.city,
  COUNT(*) as product_store_combinations,
  AVG(p.price) as avg_price,
  SUM(p.price) as total_inventory_value
FROM products p
CROSS JOIN stores s
GROUP BY s.city
ORDER BY s.city;

-- Cross join for inventory matrix
SELECT
  p.prod_name,
  SUM(CASE WHEN s.city = 'NYC' THEN 1 ELSE 0 END) as nyc_availability,
  SUM(CASE WHEN s.city = 'LA' THEN 1 ELSE 0 END) as la_availability,
  COUNT(s.store_id) as total_store_availability
FROM products p
CROSS JOIN stores s
GROUP BY p.prod_name, p.prod_id
ORDER BY p.prod_id;

-- Cross join with conditions and calculations
SELECT
  p.prod_name,
  s.store_name,
  p.price,
  p.price * 0.1 as store_commission,
  p.price * 1.08 as price_with_tax
FROM products p
CROSS JOIN stores s
WHERE p.price BETWEEN 25.00 AND 1000.00
ORDER BY p.price DESC, s.store_name;

DROP TABLE products;

DROP TABLE stores;

DROP TABLE categories;
