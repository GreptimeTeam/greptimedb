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

GreptimeDB's type system is built on Arrow/DataFusion, where each data type in GreptimeDB corresponds to a data type in Arrow/DataFusion. The proposed JSON type will be implemented on top of the existing `Binary` type, leveraging the current `datatype::value::Value` and `datatype::vectors::BinaryVector` implementations, utilizing the JSONB format as the encoding of JSON data. JSON data is stored and processed similarly to binary data within the storage layer and query engine.

This approach brings problems when dealing with insertions and queries of JSON columns.

## Insertion

Users commonly write JSON data as strings. Thus we need to make conversions between string and JSONB. There are 2 ways to do this:

1. MySQL and PostgreSQL servers provide auto-conversions between strings and JSONB. When a string is inserted into a JSON column, the server will try to parse the string as JSON and convert it to JSONB. The non-JSON strings will be rejected.

2. A function `parse_json` is provided to convert string to JSONB. If the string is not a valid JSON string, the function will return an error.

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

The dataflow of the insertion process is as follows:
```
Insert JSON strings directly through client:
        (Server identifies JSON type and performs auto-conversion)
                        Encode               Insert
        JSON Strings ┌──────────┐JSONB ┌──────────────┐JSONB
 Client ------------>│  Server  │----->│ Query Engine │-----> Storage
                     └──────────┘      └──────────────┘
Insert JSON strings through parse_json function:
                              (Conversion is performed by function inside Query Engine)
                                               Encode & Insert
        JSON Strings ┌──────────┐JSON Strings ┌──────────────┐JSONB
 Client ------------>│  Server  │------------>│ Query Engine │-----> Storage
                     └──────────┘             └──────────────┘
```

However, insertions through prepared statements in MySQL clients will not trigger the auto-conversion since the prepared plan of datafusion cannot identify JSON type from binary type. The server will directly insert the input string into the JSON column as string bytes instead of converting it to JSONB. This may cause problems when the string is not a valid JSON string.

```
For insertions through prepared statements in MySQL clients:
       Prepare stmt ┌──────────┐
Client ------------>│  Server  │ -----> Cached Plan ───────┐
                    └──────────┘                           │
   (Cached plan erased type info of JSON                   │
            and treat it as binary)                        │
                         ┌─────────────────────────────────┘
                         ↓
       Execute stmt ┌──────────┐
Client ------------>│  Server  │ (Cannot perform auto-conversion here)
                    └──────────┘
```

Thus, following codes may not work as expected:
```Rust
// sqlx first prepare a statement and then execute it.
sqlx::query(create table test (ts TIMESTAMP TIME INDEX, b JSON))
    .execute(&pool)
    .await?;
sqlx::query("insert into demo values(?, ?)")
    .bind(0)
    .bind(r#"{"name": "jHl2oDDnPc1i2OzlP5Y", "timestamp": "2024-07-25T04:33:11.369386Z", "attributes": { "event_attributes": 48.28667 }}"#)
    .execute(&pool)
    .await?;
```
The JSON will be inserted as string bytes instead of JSONB. Also happens when using `PREPARE` and `EXECUTE` in MySQL client. Among these scenarios, we need to use `parse_json` function explicitly to convert the string to JSONB.

## Query

Correspondingly, users prefer to display JSON data as strings. Thus we need to make conversions between JSON data and strings before presenting JSON data. There are also 2 ways to do this: auto-conversions on MySQL and PostgreSQL servers, and function `json_to_string`.

For example, in MySQL client:
```SQL
SELECT b FROM test;

SELECT json_to_string(b) FROM test;
```
Will both return the JSON as human-readable strings.

Specifically, to perform auto-conversions, we attach a message to JSON data in the `metadata` of `Field` in Arrow/Datafusion schema when scanning a JSON column. Frontend servers could identify JSON data and convert it to strings.

The dataflow of the query process is as follows:
```
Query directly through client:
    (Server identifies JSON type and performs auto-conversion based on column metadata)
                        Decode               Scan
        JSON Strings ┌──────────┐JSONB ┌──────────────┐JSONB
 Client ------------>│  Server  │----->│ Query Engine │<----- Storage
                     └──────────┘      └──────────────┘
Query through json_to_string function:
                                (Conversion is performed by function inside Query Engine)
                                               Scan & Decode
        JSON Strings ┌──────────┐JSON Strings ┌──────────────┐JSONB
 Client ------------>│  Server  │------------>│ Query Engine │-----> Storage
                     └──────────┘             └──────────────┘
```

However, if a function uses JSON type as its return type, the metadata method mentioned above is not applicable. Thus the functions of JSON type should specify the return type explicitly instead of returning a JSON type, such as `json_get_int` and `json_get_float` which return corresponding data of `INT` and `FLOAT` type respectively.

## Functions
Similar to the common JSON type, JSON data can be queried with functions.

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

The auto-conversion mechanism is not supported in all scenarios. We need to find workarounds for these scenarios.

# Alternatives

Extract and flatten JSON schema to store in a structured format through pipeline. For nested data, we can provide nested types like `STRUCT` or `ARRAY`.
