-- Migrated from DuckDB test: test/sql/join/right_outer/test_right_outer.test
-- Tests RIGHT OUTER JOIN scenarios

CREATE TABLE products_right("id" INTEGER, "name" VARCHAR, price DOUBLE, ts TIMESTAMP TIME INDEX);

CREATE TABLE inventory_right(product_id INTEGER, stock INTEGER, "warehouse" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO products_right VALUES (1, 'Laptop', 1000.0, 1000), (2, 'Mouse', 25.0, 2000);

INSERT INTO inventory_right VALUES (1, 5, 'WH1', 3000), (2, 10, 'WH1', 4000), (3, 15, 'WH2', 5000);

-- Basic RIGHT JOIN
SELECT * FROM products_right p RIGHT JOIN inventory_right i ON p."id" = i.product_id ORDER BY i.product_id;

-- RIGHT JOIN with WHERE on left table
SELECT * FROM products_right p RIGHT JOIN inventory_right i ON p."id" = i.product_id WHERE p.price IS NULL ORDER BY i.product_id;

-- RIGHT JOIN with aggregation
SELECT i."warehouse", COUNT(*) as items, AVG(p.price) as avg_price
FROM products_right p RIGHT JOIN inventory_right i ON p."id" = i.product_id
GROUP BY i."warehouse" ORDER BY i."warehouse";

DROP TABLE inventory_right;

DROP TABLE products_right;
