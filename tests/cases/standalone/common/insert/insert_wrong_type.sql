-- test for issue #3235
CREATE TABLE b(i interval, ts timestamp time index);
-- should fail
INSERT INTO b VALUES ('1 year', 1000);
-- success
INSERT INTO b VALUES (interval '1 year', 1000);

DROP TABLE b;
