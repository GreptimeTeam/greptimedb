CREATE TABLE table_should_not_break_after_incorrect_alter(i INTEGER, j TIMESTAMP TIME INDEX);

ALTER TABLE table_should_not_break_after_incorrect_alter ADD column k string NOT NULL;

INSERT INTO table_should_not_break_after_incorrect_alter VALUES (1, 1), (2, 2);

SELECT * FROM table_should_not_break_after_incorrect_alter;

DROP TABLE table_should_not_break_after_incorrect_alter;
