-- SQLNESS PROTOCOL MYSQL
SELECT * FROM timestamps;

-- SQLNESS PROTOCOL MYSQL
SELECT ts::BIGINT AS epoch_ns, ts::STRING AS timestamp_string FROM timestamps;
