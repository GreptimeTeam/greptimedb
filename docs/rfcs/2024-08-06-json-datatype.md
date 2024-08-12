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

SELECT json_get_by_paths_int(b, 'attributes', 'event_attributes') + 1 FROM test;
+-------------------------------+
| b.attributes.event_attributes |
+-------------------------------+
|                      49.28667 |
+-------------------------------+

```

## Storage and Query

Data of JSON type is stored as JSONB format in the database. For storage layer and query engine, data is represented as a binary array and can be queried through pre-defined JSON functions. For clients, data is shown as strings.

Insertions of JSON goes through following steps:

1. Client gets JSON strings and sends it to the frontend.
2. Frontend encode JSON strings to JSONB format and sends it to the datanode.
3. Datanode stores binary data in the database.

```
Insertion:
                          Encode                    Store
         JSON Strings ┌────────────┐ JSONB Data ┌────────────┐
 client ------------->│  Frontend  │----------->│  Datanode  │--> Storage
                      └────────────┘            └────────────┘
```

The data of JSON type is represented by `Binary` data type in arrow. There are 2 types of JSON queries: get json elements through keys and compute over json elements.

For the former, the query engine performs queries directly over binary data. We provide functions like `json_get` and `json_get_by_paths` to extract json elements through keys.

For the latter, users need to manually specify the data type of the json elements for computing. We provide functions like `as_int` and `as_double` to decode the binary data into data with specified data type for further computation.

Queries of JSON goes through following steps:

1. Client sends query to frontend, and frontend sends it to datafusion, which is the query engine of GreptimeDB.
2. Datafusion performs query over JSON data, and returns binary data to frontend.
3. If no computation is needed, frontend directly decodes it to JSON strings and return it to clients.
4. If computation is needed, the binary data is decoded and converted to the specified data type to perform computation. There's no need for further decoding in the frontend.

```
Queries without computation, decoding in frontend:
                          Decode                     Query
         JSON Strings ┌────────────┐ JSONB Data ┌──────────────┐
 client <-------------│  Frontend  │<-----------│  Datafusion  │<-- Storage
                      └────────────┘            └──────────────┘

Queries with computation, decoding in datafusion:
                                                                         Query
         Data of Specified Type ┌────────────┐ Data of Certain Type ┌──────────────┐
 client <-----------------------│  Frontend  │<---------------------│  Datafusion  │<-- Storage
                                └────────────┘                      └──────────────┘
```

# Drawbacks

As a general purpose data type, JSONB may not be as efficient as specialized data types for specific scenarios.

# Alternatives

Extract and flatten JSON schema to store in a structured format through pipeline. For nested data, we can provide nested types like `STRUCT` or `ARRAY`.
