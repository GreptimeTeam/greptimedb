-- Migrated from DuckDB test: test/sql/join/inner/test_range_join.test
-- Tests inequality JOIN conditions

CREATE TABLE events("id" INTEGER, event_time INTEGER, duration INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE time_ranges(start_time INTEGER, end_time INTEGER, range_name VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO events VALUES (1, 10, 5, 1000), (2, 25, 3, 2000), (3, 45, 8, 3000);

INSERT INTO time_ranges VALUES (0, 20, 'Early', 4000), (20, 40, 'Mid', 5000), (40, 60, 'Late', 6000);

-- Range join using BETWEEN
SELECT e."id", e.event_time, t.range_name
FROM events e JOIN time_ranges t ON e.event_time BETWEEN t.start_time AND t.end_time
ORDER BY e."id";

-- Inequality join conditions
SELECT e."id", e.event_time, e.duration, t.range_name
FROM events e JOIN time_ranges t ON e.event_time >= t.start_time AND e.event_time < t.end_time
ORDER BY e."id";

-- Join with overlap condition
SELECT e."id", t.range_name
FROM events e JOIN time_ranges t ON 
  e.event_time < t.end_time AND (e.event_time + e.duration) > t.start_time
ORDER BY e."id", t.start_time;

-- Self join with inequality
SELECT e1."id" as id1, e2."id" as id2, e1.event_time, e2.event_time
FROM events e1 JOIN events e2 ON e1.event_time < e2.event_time
ORDER BY e1."id", e2."id";

DROP TABLE time_ranges;

DROP TABLE events;