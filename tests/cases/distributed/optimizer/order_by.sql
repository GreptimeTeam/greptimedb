-- SQLNESS REPLACE (peers.*) REDACTED
explain select * from numbers;

-- SQLNESS REPLACE (peers.*) REDACTED
explain select * from numbers order by number desc;

-- SQLNESS REPLACE (peers.*) REDACTED
explain select * from numbers order by number asc;

-- SQLNESS REPLACE (peers.*) REDACTED
explain select * from numbers order by number desc limit 10;

-- SQLNESS REPLACE (peers.*) REDACTED
explain select * from numbers order by number asc limit 10;
