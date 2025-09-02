-- Tests joins combined with window functions

CREATE TABLE sales_data_win(rep_id INTEGER, sale_amount DOUBLE, sale_date DATE, ts TIMESTAMP TIME INDEX);

CREATE TABLE rep_targets(rep_id INTEGER, quarterly_target DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO sales_data_win VALUES (1, 1000, '2023-01-01', 1000), (1, 1500, '2023-01-15', 2000), (2, 800, '2023-01-02', 3000), (2, 1200, '2023-01-16', 4000), (3, 2000, '2023-01-03', 5000);

INSERT INTO rep_targets VALUES (1, 5000, 1000), (2, 4000, 2000), (3, 6000, 3000);

SELECT s.rep_id, s.sale_amount, rt.quarterly_target, SUM(s.sale_amount) OVER (PARTITION BY s.rep_id ORDER BY s.sale_date) as running_total FROM sales_data_win s INNER JOIN rep_targets rt ON s.rep_id = rt.rep_id ORDER BY s.rep_id, s.sale_date;

DROP TABLE sales_data_win;

DROP TABLE rep_targets;