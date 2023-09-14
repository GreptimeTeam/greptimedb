CREATE TABLE integers(i INTEGER, j TIMESTAMP TIME INDEX);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT * FROM integers WHERE i IN ((SELECT i FROM integers)) ORDER BY i;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT * FROM integers i1 WHERE EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i1.i;

create table other (i INTEGER, j TIMESTAMP TIME INDEX);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
explain select t.i
from (
    select * from integers join other on 1=1
) t
where t.i is not null
order by t.i desc;

EXPLAIN INSERT INTO other SELECT i, 2 FROM integers WHERE i=(SELECT MAX(i) FROM integers);

drop table other;

drop table integers;
