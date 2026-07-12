-- Tests joins with DISTINCT operations

CREATE TABLE products_dist(prod_id INTEGER, prod_name VARCHAR, "category" VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE sales_dist(sale_id INTEGER, prod_id INTEGER, customer VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO products_dist VALUES (1, 'Widget', 'Tools', 1000), (2, 'Gadget', 'Electronics', 2000);

INSERT INTO sales_dist VALUES (1, 1, 'Alice', 1000), (2, 1, 'Bob', 2000), (3, 2, 'Alice', 3000), (4, 1, 'Alice', 4000);

SELECT DISTINCT p.category FROM products_dist p INNER JOIN sales_dist s ON p.prod_id = s.prod_id ORDER BY p.category;

SELECT DISTINCT s.customer, p.category FROM sales_dist s INNER JOIN products_dist p ON s.prod_id = p.prod_id ORDER BY s.customer, p.category;
SELECT p.prod_name, COUNT(DISTINCT s.customer) as unique_customers FROM products_dist p LEFT JOIN sales_dist s ON p.prod_id = s.prod_id GROUP BY p.prod_id, p.prod_name ORDER BY unique_customers DESC;

DROP TABLE products_dist;

DROP TABLE sales_dist;
