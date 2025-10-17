-- String FORMAT function tests

-- Basic FORMAT function
SELECT FORMAT('Hello {}', 'world');

SELECT FORMAT('Number: {}', 42);

SELECT FORMAT('{} + {} = {}', 2, 3, 5);

-- FORMAT with named placeholders
SELECT FORMAT('Hello {name}', 'name', 'world');

SELECT FORMAT('{first} {last}', 'first', 'John', 'last', 'Doe');

-- FORMAT with different data types
SELECT FORMAT('Value: {}, Date: {}', 123.45, '2023-01-01');

SELECT FORMAT('Boolean: {}, NULL: {}', true, NULL);

-- Test with table data
CREATE TABLE format_test("name" VARCHAR, age INTEGER, salary DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO format_test VALUES
    ('John', 30, 50000.0, 1000),
    ('Jane', 25, 60000.0, 2000),
    ('Bob', 35, 55000.0, 3000);

SELECT FORMAT('Employee: {}, Age: {}, Salary: {}', "name", age, salary) FROM format_test ORDER BY ts;

SELECT FORMAT('{} is {} years old', "name", age) FROM format_test ORDER BY ts;

DROP TABLE format_test;

-- Test escape characters (your new feature)
SELECT FORMAT('Path: {{root}}/{dir}/file-{}.txt', 'dir', 'etc', 42);

SELECT FORMAT('JSON: {{"key": "{value}"}}', 'value', 'test');

SELECT FORMAT('Braces: {{ and }} around {}', 'content');

-- Test mixed positional and named with escapes
SELECT FORMAT('Config: {{database: "{db}", user: "{}", table: "{table}"}}', 'admin', 'db', 'mydb', 'table', 'users');

-- Test insufficient arguments (graceful degradation)
SELECT FORMAT('A:{} B:{} C:{}', 'first', 'second');

SELECT FORMAT('Named {x} and {y}', 'x', 'value1');

SELECT FORMAT('Position {} Named {name}', 'pos_val', 'name', 'named_val', 'extra');

-- Test complex data types formatting
CREATE TABLE format_complex(
    "id" INTEGER,
    score DECIMAL(10,2), 
    created_at TIMESTAMP,
    d DATE,
    is_active BOOLEAN,
    ts TIMESTAMP TIME INDEX
);

INSERT INTO format_complex VALUES
    (1, 95.50, '2024-01-01 10:00:00', '2024-01-01', true, 1000),
    (2, 87.25, '2024-01-02 15:30:00', '2024-01-02', false, 2000);

-- Test formatting various data types
SELECT FORMAT('ID:{} Score:{} Created:{} Active:{}', "id", score, created_at, is_active) 
FROM format_complex ORDER BY ts;

SELECT FORMAT('Record {id}: {status} user with score {score} on {date}', 
              'id', "id", 
              'status', CASE WHEN is_active THEN 'active' ELSE 'inactive' END,
              'score', score,
              'date', d) 
FROM format_complex ORDER BY ts;

-- With timezone
set time_zone='Asia/Shanghai';

SELECT FORMAT('Created at: {} ,date {}', created_at, d) 
FROM format_complex ORDER BY ts;

set time_zone='UTC';

-- Test with very large strings
SELECT FORMAT('Repeat: {}', REPEAT('A', 1000));

-- Test edge case: only escape characters
SELECT FORMAT('{{}}');

SELECT FORMAT('{{text}}');

-- Test complex nested scenarios
SELECT FORMAT('User {user} logged {action} at {time} with result: {{"status": "{status}", "code": {}}}',
              'user', 'admin',
              'action', 'in', 
              'time', '2024-01-01',
              'status', 'success',
              200);

-- Test empty and whitespace placeholders (should be treated as positional)
SELECT FORMAT('A:{} B:{ } C:{  }', 'val1', 'val2', 'val3');

DROP TABLE format_complex;
