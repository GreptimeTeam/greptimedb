# Log benchmark configuration
This repo holds the configuration we used to benchmark GreptimeDB, Clickhouse and Elastic Search.

Here are the versions of databases we used in the benchmark

| name          | version    |
| :------------ | :--------- |
| GreptimeDB    | v0.9.2     |
| Clickhouse    | 24.9.1.219 |
| Elasticsearch | 8.15.0     |

## Structured model vs Unstructured model
We divide test into two parts, using structured model and unstructured model accordingly. You can also see the difference in create table clause.

__Structured model__

The log data is pre-processed into columns by vector. For example an insert request looks like following
```SQL
INSERT INTO test_table (bytes, http_version, ip, method, path, status, user, timestamp) VALUES ()
```
The goal is to test string/text support for each database. In real scenarios it means the datasource(or log data producers) have separate fields defined, or have already processed the raw input.

__Unstructured model__

The log data is inserted as a long string, and then we build fulltext index upon these strings. For example an insert request looks like following
```SQL
INSERT INTO test_table (message, timestamp) VALUES ()
```
The goal is to test fuzzy search performance for each database. In real scenarios it means the log is produced by some kind of middleware and inserted directly into the database.

## Creating tables
See [here](./create_table.sql) for GreptimeDB and Clickhouse's create table clause.
The mapping of Elastic search is created automatically.

## Vector Configuration
We use vector to generate random log data and send inserts to databases.
Please refer to [structured config](./structured_vector.toml) and [unstructured config](./unstructured_vector.toml) for detailed configuration.

## SQLs and payloads
Please refer to [SQL query](./query.sql) for GreptimeDB and Clickhouse, and [query payload](./query.md) for Elastic search.

## Steps to reproduce
0. Decide whether to run structured model test or unstructured mode test.
1. Build vector binary(see vector's config file for specific branch) and databases binaries accordingly.
2. Create table in GreptimeDB and Clickhouse in advance.
3. Run vector to insert data.
4. When data insertion is finished, run queries against each database. Note: you'll need to update timerange value after data insertion.

## Addition
- You can tune GreptimeDB's configuration to get better performance.
- You can setup GreptimeDB to use S3 as storage, see [here](https://docs.greptime.com/user-guide/deployments/configuration#storage-options).
