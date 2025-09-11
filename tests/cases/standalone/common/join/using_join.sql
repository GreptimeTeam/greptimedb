-- Migrated from DuckDB test: test/sql/join/inner/test_using_join.test
-- Tests JOIN USING clause

CREATE TABLE users_join(user_id INTEGER, username VARCHAR, email VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE orders_join(order_id INTEGER, user_id INTEGER, amount DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO users_join VALUES (1, 'alice', 'alice@test.com', 1000), (2, 'bob', 'bob@test.com', 2000);

INSERT INTO orders_join VALUES (101, 1, 150.0, 3000), (102, 1, 200.0, 4000), (103, 2, 75.0, 5000);

-- JOIN USING (automatically joins on common column)
SELECT * FROM users_join JOIN orders_join USING (user_id) ORDER BY order_id;

-- LEFT JOIN USING
SELECT * FROM users_join LEFT JOIN orders_join USING (user_id) ORDER BY user_id, order_id NULLS LAST;

-- JOIN USING with WHERE
SELECT * FROM users_join JOIN orders_join USING (user_id) WHERE amount > 100 ORDER BY amount;

-- Multiple table JOIN USING
CREATE TABLE user_profiles(user_id INTEGER, age INTEGER, city VARCHAR, ts TIMESTAMP TIME INDEX);
INSERT INTO user_profiles VALUES (1, 25, 'NYC', 6000), (2, 30, 'LA', 7000);

SELECT * FROM users_join
JOIN orders_join USING (user_id)
JOIN user_profiles USING (user_id)
ORDER BY order_id;

DROP TABLE user_profiles;

DROP TABLE orders_join;

DROP TABLE users_join;
