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

## User Interface
The feature introduces a new data type for the database, similar to the common JSON type. Data is written as JSON strings and can be queried using functions.

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

SELECT json_get(b, 'name') FROM test;
+---------------------+
| b.name              |
+---------------------+
| jHl2oDDnPc1i2OzlP5Y |
+---------------------+

SELECT json_get_by_paths(b, 'attributes', 'event_attributes') + 1 FROM test;
+-------------------------------+
| b.attributes.event_attributes |
+-------------------------------+
|                      49.28667 |
+-------------------------------+

```

## Storage and Querying

Data of JSON type is stored as JSONB format in the database. For storage layer, data is represented as a binary array and can be queried through pre-defined JSON functions. For clients, data is shown as strings and can be casted to other types if needed.

# Drawbacks

As a general purpose data type, JSONB may not be as efficient as specialized data types for specific scenarios.

# Alternatives

Extract and flatten JSON schema to store in a structured format through pipeline. For nested data, we can provide nested types like `STRUCT` or `ARRAY`.
