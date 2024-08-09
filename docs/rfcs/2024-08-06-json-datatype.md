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

## Storage

### Schema Inference
Unlike other types, the schema of JSON data is inconsistent. For different JSON columns, we introduce a dynamic schema inference method for storing the data.

For example:
```JSON
{
  "a": "jHl2oDDnPc1i2OzlP5Y",
  "b": "2024-07-25T04:33:11.369386Z",
  "c": { "d": 48.28648 }
}
```
This will be parsed at runtime and stored as a corresponding `Struct` type in Arrow:
```Rust
Struct(
    Field("a", Utf8),
    Field("b", Utf8),
    Field("c", Struct(Field("d", Float64))),
)
```

Dynamic schema inference helps achieve compression in some scenarios. See [benchmark](https://github.com/CookiePieWw/json-format-in-parquet-benchmark/) for more information.

## Schema Change
The schema must remain consistent for a column within a table. When inserting data with different schemas, schema changes may occur. There are two types of schema changes:

1. Field Addition

   Newly added fields can be incorporated into the schema, treating added fields in previously inserted data as null:
   ```Rust
   Struct(
       Field("a", Utf8),
   )
   +
   Struct(
       Field("a", Utf8),
       Field("e", Int32)
   )
   =
   Struct(
       Field("a", Utf8),
       Field("e", Int32)
   )
   ```

2. Field Modification

   Compatible fields can be altered to the widest type, similar to integral promotion in C:
   ```Rust
   Struct(
       Field("a", Int16),
   )
   +
   Struct(
       Field("a", Int32),
   )
   =
   Struct(
       Field("a", Int32),
   )
   ```

   Non-compatible fields will fallback to a binary array to store the JSONB encoding:
   ```Rust
   Struct(
       Field("a", Struct(Field("b", Float64))),
   )
   +
   Struct(
       Field("a", Int32),
   )
   =
   Struct(
       Field("a", BinaryArray), // JSONB
   )
   ```

Like schema inference, schema changes are performed automatically without manual configuration.

# Drawbacks

1. This datatype is best suited for data with similar schemas. Varying schemas can lead to frequent schema changes and fallback to JSONB.
2. Schema inference and change bring additional writing overhead in favor of better compression rate.

# Alternatives

1. JSONB, a widely used binary representation format of json.
2. JSONC: A tape representation format for JSON with similar writing and query performance and better compression in some cases. See [discussion](https://github.com/apache/datafusion/issues/7845#issuecomment-2068061465) and [repo](https://github.com/CookiePieWw/jsonc) for more information.
