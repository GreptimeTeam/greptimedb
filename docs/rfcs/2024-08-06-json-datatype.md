---
Feature Name: Json Datatype
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/4230
Date: 2024-8-6
Author: "Yuhan Wang <profsyb@gmail.com>"
---

# Summary
This RFC proposes a method for storing and querying JSON data in the database.

# Motivation
JSON is widely used across various scenarios. Direct support for writing and querying JSON can significantly enhance the database's flexibility.

# Details

## Storage and Query

The type system of GreptimeDB is based on the types of arrow/datafusion, each type has a corresponding physical type from arrow/datafusion. Thus, the json type is built on top of the `Binary` type, utilizing current implementation of both `Value` and `Vector` of it. JSON type performs the same as Binary type inside the storage layer and query engine.

This also brings 2 problems: insertion and query interface.

## Insertion

User commonly write JSON data as strings. Thus we need to make conversion between string and binary data. There are 2 ways to do this:

1. MySQL and PostgreSQL servers provide auto-conversion between string and JSON data. When a string is inserted into a JSON column, the server will try to parse the string as JSON data and convert it to binary data of JSON type. The non-JSON string will be rejected.

2. A function `parse_json` is provided to convert string to JSON data. The function will return a binary data of JSON type. If the string is not a valid JSON string, the function will return an error.

For example, in MySQL client:
```SQL
CREATE TABLE IF NOT EXISTS test (
    ts TIMESTAMP TIME INDEX,
    a INT,
    b JSON
);

INSERT INTO test VALUES(
    0,
    0,
    '{
        "name": "jHl2oDDnPc1i2OzlP5Y",
        "timestamp": "2024-07-25T04:33:11.369386Z",
        "attributes": { "event_attributes": 48.28667 }
    }'
);

INSERT INTO test VALUES(
    0,
    0,
    parse_json('{
        "name": "jHl2oDDnPc1i2OzlP5Y",
        "timestamp": "2024-07-25T04:33:11.369386Z",
        "attributes": { "event_attributes": 48.28667 }
    }')
);
```
Are both valid.

For former the conversion is done by the server, while for the latter the conversion is done by the query engine.

## Query Interface

Correspondingly, users prefer to display JSON data as strings. Thus we need to make conversion between binary data and string data. There are alsol 2 ways to do this: auto-conversions on MySQL and PostgreSQL servers, and function `json_to_string`.

For example, in MySQL client:
```SQL
SELECT b FROM test;

SELECT json_to_string(b) FROM test;
```
Will both return the JSON string.

Specifically, we attach a message to the binary data of JSON type in the `metadata` of `Field` in arrow/datafusion schema. Frontend servers could identify the type of the binary data and convert it to string data if necessary. But for functions with a JSON return type, the metadata method is not applicable. Thus the functions of JSON type should specify the return type explicitly, such as `json_get_int` and `json_get_float` which return `INT` and `FLOAT` respectively.

## Functions
Similar to the common JSON type, data is written as JSON strings and can be queried with functions.

For example:
```SQL
CREATE TABLE IF NOT EXISTS test (
    ts TIMESTAMP TIME INDEX,
    a INT,
    b JSON
);

INSERT INTO test VALUES(
    0,
    0,
    '{
        "name": "jHl2oDDnPc1i2OzlP5Y",
        "timestamp": "2024-07-25T04:33:11.369386Z",
        "attributes": { "event_attributes": 48.28667 }
    }'
);

SELECT json_get_int(b, 'name') FROM test;
+---------------------+
| b.name              |
+---------------------+
| jHl2oDDnPc1i2OzlP5Y |
+---------------------+

SELECT json_get_float(b, 'attributes.event_attributes') FROM test;
+--------------------------------+
| b.attributes.event_attributes  |
+--------------------------------+
| 48.28667                       |
+--------------------------------+

```
And more functions can be added in the future.

# Drawbacks

As a general purpose JSON data type, JSONB may not be as efficient as specialized data types for specific scenarios.

# Alternatives

Extract and flatten JSON schema to store in a structured format through pipeline. For nested data, we can provide nested types like `STRUCT` or `ARRAY`.
