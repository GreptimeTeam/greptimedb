-- Migrated from DuckDB test: test/sql/join/iejoin/ inequality join tests
-- Tests inequality join conditions

CREATE TABLE time_events(event_id INTEGER, event_time TIMESTAMP, event_type VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE time_windows(window_id INTEGER, start_time TIMESTAMP, end_time TIMESTAMP, window_name VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO time_events VALUES 
(1, '2023-01-01 10:15:00', 'login', 1000),
(2, '2023-01-01 10:30:00', 'purchase', 2000),
(3, '2023-01-01 10:45:00', 'logout', 3000),
(4, '2023-01-01 11:05:00', 'login', 4000),
(5, '2023-01-01 11:20:00', 'view', 5000),
(6, '2023-01-01 11:35:00', 'purchase', 6000);

INSERT INTO time_windows VALUES 
(1, '2023-01-01 10:00:00', '2023-01-01 10:30:00', 'Morning Early', 1000),
(2, '2023-01-01 10:30:00', '2023-01-01 11:00:00', 'Morning Late', 2000),
(3, '2023-01-01 11:00:00', '2023-01-01 11:30:00', 'Noon Early', 3000),
(4, '2023-01-01 11:30:00', '2023-01-01 12:00:00', 'Noon Late', 4000);

-- Range join: events within time windows
SELECT 
  e.event_id, e.event_time, e.event_type, w.window_name
FROM time_events e
INNER JOIN time_windows w 
  ON e.event_time >= w.start_time AND e.event_time < w.end_time
ORDER BY e.event_time;

-- Inequality join with additional conditions
SELECT 
  e.event_id, e.event_type, w.window_name
FROM time_events e
INNER JOIN time_windows w 
  ON e.event_time >= w.start_time 
  AND e.event_time < w.end_time
  AND e.event_type = 'purchase'
ORDER BY e.event_time;

-- Cross-time analysis with inequality joins
SELECT 
  e1.event_id as first_event, e2.event_id as second_event,
  e1.event_type as first_type, e2.event_type as second_type,
  e2.event_time - e1.event_time as time_diff
FROM time_events e1
INNER JOIN time_events e2 
  ON e1.event_time < e2.event_time 
  AND e2.event_time - e1.event_time <= INTERVAL '30 minutes'
ORDER BY e1.event_time, e2.event_time;

CREATE TABLE price_history(item_id INTEGER, price DOUBLE, effective_date DATE, ts TIMESTAMP TIME INDEX);
CREATE TABLE orders_ineq(order_id INTEGER, item_id INTEGER, order_date DATE, ts TIMESTAMP TIME INDEX);

INSERT INTO price_history VALUES 
(1, 100.00, '2023-01-01', 1000), (1, 110.00, '2023-01-15', 2000),
(2, 50.00, '2023-01-01', 3000), (2, 55.00, '2023-01-20', 4000);

INSERT INTO orders_ineq VALUES 
(1, 1, '2023-01-10', 1000), (2, 1, '2023-01-20', 2000),
(3, 2, '2023-01-05', 3000), (4, 2, '2023-01-25', 4000);

-- Historical price lookup with inequality join
SELECT 
  o.order_id, o.order_date, p.price, p.effective_date
FROM orders_ineq o
INNER JOIN price_history p 
  ON o.item_id = p.item_id 
  AND o.order_date >= p.effective_date
ORDER BY o.order_id;

-- Latest price before order date
SELECT 
  o.order_id, o.order_date, latest_price.price
FROM orders_ineq o
INNER JOIN (
  SELECT 
    item_id, 
    price,
    effective_date,
    ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY effective_date DESC) as rn
  FROM price_history
) latest_price 
  ON o.item_id = latest_price.item_id 
  AND o.order_date >= latest_price.effective_date
  AND latest_price.rn = 1
ORDER BY o.order_id;

DROP TABLE time_events;

DROP TABLE time_windows;

DROP TABLE price_history;

DROP TABLE orders_ineq;