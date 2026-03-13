-- Tests join predicate pushdown optimization scenarios

CREATE TABLE events_push(event_id INTEGER, user_id INTEGER, event_type VARCHAR, "value" INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE users_push(user_id INTEGER, user_name VARCHAR, "region" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO events_push VALUES (1, 100, 'click', 1, 1000), (2, 100, 'view', 2, 2000), (3, 200, 'click', 1, 3000), (4, 300, 'purchase', 5, 4000);

INSERT INTO users_push VALUES (100, 'Alice', 'US', 1000), (200, 'Bob', 'EU', 2000), (300, 'Charlie', 'US', 3000);

SELECT e.event_type, u.region, COUNT(*) as event_count FROM events_push e INNER JOIN users_push u ON e.user_id = u.user_id WHERE u.region = 'US' GROUP BY e.event_type, u.region ORDER BY event_count DESC, e.event_type ASC;

SELECT u.user_name, SUM(e."value") as total_value FROM users_push u INNER JOIN events_push e ON u.user_id = e.user_id WHERE e.event_type = 'click' GROUP BY u.user_id, u.user_name ORDER BY total_value DESC, u.user_name ASC;

DROP TABLE events_push;

DROP TABLE users_push;
