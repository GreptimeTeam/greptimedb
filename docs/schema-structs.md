# Schema Structs

# Common Schemas
The `datatypes` crate defines the elementary schema struct to describe the metadata.

## ColumnSchema
[ColumnSchema](https://github.com/GreptimeTeam/greptimedb/blob/9fa871a3fad07f583dc1863a509414da393747f8/src/datatypes/src/schema/column_schema.rs#L36) represents the metadata of a column. It is equivalent to arrow's [Field](https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html) with additional metadata such as default constraint and whether the column is a time index. The time index is the column with a `TIME INDEX` constraint of a table. We can convert the `ColumnSchema` into an arrow `Field` and convert the `Field` back to the `ColumnSchema` without losing metadata.

```rust
pub struct ColumnSchema {
    pub name: String,
    pub data_type: ConcreteDataType,
    is_nullable: bool,
    is_time_index: bool,
    default_constraint: Option<ColumnDefaultConstraint>,
    metadata: Metadata,
}
```

## Schema
[Schema](https://github.com/GreptimeTeam/greptimedb/blob/9fa871a3fad07f583dc1863a509414da393747f8/src/datatypes/src/schema.rs#L38) is an ordered sequence of `ColumnSchema`. It is equivalent to arrow's [Schema](https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html) with additional metadata including the index of the time index column and the version of this schema. Same as `ColumnSchema`, we can convert our `Schema` from/to arrow's `Schema`.

```rust
use arrow::datatypes::Schema as ArrowSchema;

pub struct Schema {
    column_schemas: Vec<ColumnSchema>,
    name_to_index: HashMap<String, usize>,
    arrow_schema: Arc<ArrowSchema>,
    timestamp_index: Option<usize>,
    version: u32,
}

pub type SchemaRef = Arc<Schema>;
```

We alias `Arc<Schema>` as `SchemaRef` since it is used frequently. Mostly, we use our `ColumnSchema` and `Schema` structs instead of Arrow's `Field` and `Schema` unless we need to invoke third-party libraries (like DataFusion or ArrowFlight) that rely on Arrow.

## RawSchema
`Schema` contains fields like a map from column names to their indices in the `ColumnSchema` sequences and a cached arrow `Schema`. We can construct these fields from the `ColumnSchema` sequences thus we don't want to serialize them. This is why we don't derive `Serialize` and `Deserialize` for `Schema`. We introduce a new struct [RawSchema](https://github.com/GreptimeTeam/greptimedb/blob/9fa871a3fad07f583dc1863a509414da393747f8/src/datatypes/src/schema/raw.rs#L24) which keeps all required fields of a `Schema` and derives the serialization traits. To serialize a `Schema`, we need to convert it into a `RawSchema` first and serialize the `RawSchema`.

```rust
pub struct RawSchema {
    pub column_schemas: Vec<ColumnSchema>,
    pub timestamp_index: Option<usize>,
    pub version: u32,
}
```

We want to keep the `Schema` simple and avoid putting too much business-related metadata in it as many different structs or traits rely on it.

# Schema of the Table
A table maintains its schema in [TableMeta](https://github.com/GreptimeTeam/greptimedb/blob/9fa871a3fad07f583dc1863a509414da393747f8/src/table/src/metadata.rs#L97).
```rust
pub struct TableMeta {
    pub schema: SchemaRef,
    pub primary_key_indices: Vec<usize>,
    pub value_indices: Vec<usize>,
    // ...
}
```

The order of columns in `TableMeta::schema` is the same as the order specified in the `CREATE TABLE` statement which users use to create this table.

The field `primary_key_indices` stores indices of primary key columns. The field `value_indices` records the indices of value columns (non-primary key and time index, we sometimes call them field columns).

Suppose we create a table with the following SQL
```sql
CREATE TABLE cpu (
    ts TIMESTAMP,
    host STRING,
    usage_user DOUBLE,
    usage_system DOUBLE,
    datacenter STRING,
    TIME INDEX (ts),
    PRIMARY KEY(datacenter, host)) ENGINE=mito WITH(regions=1);
```

Then the table's `TableMeta` may look like this:
```json
{
    "schema":{
        "column_schemas":[
            "ts",
            "host",
            "usage_user",
            "usage_system",
            "datacenter"
        ],
        "time_index":0,
        "version":0
    },
    "primary_key_indices":[
        4,
        1
    ],
    "value_indices":[
        2,
        3
    ]
}
```


# Schemas of the storage engine
We split a table into one or more units with the same schema and then store these units in the storage engine. Each unit is a region in the storage engine.

The storage engine maintains schemas of regions in more complicated ways because it
- adds internal columns that are invisible to users to store additional metadata for each row
- provides a data model similar to the key-value model so it organizes columns in a different order
- maintains additional metadata like column id or column family

So the storage engine defines several schema structs:
- RegionSchema
- StoreSchema
- ProjectedSchema

## RegionSchema
A [RegionSchema](https://github.com/GreptimeTeam/greptimedb/blob/9fa871a3fad07f583dc1863a509414da393747f8/src/storage/src/schema/region.rs#L37) describes the schema of a region.

```rust
pub struct RegionSchema {
    user_schema: SchemaRef,
    store_schema: StoreSchemaRef,
    columns: ColumnsMetadataRef,
}
```

Each region reserves some columns called `internal columns` for internal usage:
- `__sequence`, sequence number of a row
- `__op_type`, operation type of a row, such as `PUT` or `DELETE`
- `__version`, user-specified version of a row, reserved but not used. We might remove this in the future

The table engine can't see the `__sequence` and `__op_type` columns, so the `RegionSchema` itself maintains two internal schemas:
- User schema, a `Schema` struct that doesn't have internal columns
- Store schema, a `StoreSchema` struct that has internal columns

The `ColumnsMetadata` struct keeps metadata about all columns but most time we only need to use metadata in user schema and store schema, so we just ignore it. We may remove this struct in the future.

`RegionSchema` organizes columns in the following order:
```
key columns, timestamp, [__version,] value columns, __sequence, __op_type
```

We can ignore the `__version` column because it is disabled now:

```
key columns, timestamp, value columns, __sequence, __op_type
```

Key columns are columns of a table's primary key. Timestamp is the time index column. A region sorts all rows by key columns, timestamp, sequence, and op type.

So the `RegionSchema` of our `cpu` table above looks like this:
```json
{
    "user_schema":[
        "datacenter",
        "host",
        "ts",
        "usage_user",
        "usage_system"
    ],
    "store_schema":[
        "datacenter",
        "host",
        "ts",
        "usage_user",
        "usage_system",
        "__sequence",
        "__op_type"
    ]
}
```

## StoreSchema
As described above, a [StoreSchema](https://github.com/GreptimeTeam/greptimedb/blob/9fa871a3fad07f583dc1863a509414da393747f8/src/storage/src/schema/store.rs#L36) is a schema that knows all internal columns.
```rust
struct StoreSchema {
    columns: Vec<ColumnMetadata>,
    schema: SchemaRef,
    row_key_end: usize,
    user_column_end: usize,
}
```

The columns in the `columns` and `schema` fields have the same order. The `ColumnMetadata` has metadata like column id, column family id, and comment. The `StoreSchema` also stores this metadata in `StoreSchema::schema`, so we can convert the `StoreSchema` between arrow's `Schema`. We use this feature to persist the `StoreSchema` in the SST since our SST format is `Parquet`, which can take arrow's `Schema` as its schema.

The `StoreSchema` of the region above is similar to this:
```json
{
    "schema":{
        "column_schemas":[
            "datacenter",
            "host",
            "ts",
            "usage_user",
            "usage_system",
            "__sequence",
            "__op_type"
        ],
        "time_index":2,
        "version":0
    },
    "row_key_end":3,
    "user_column_end":5
}
```

The key and timestamp columns form row keys of rows. We put them together so we can use `row_key_end` to get indices of all row key columns. Similarly, we can use the `user_column_end` to get indices of all user columns (non-internal columns).
```rust
impl StoreSchema {
    #[inline]
    pub(crate) fn row_key_indices(&self) -> impl Iterator<Item = usize> {
        0..self.row_key_end
    }

    #[inline]
    pub(crate) fn value_indices(&self) -> impl Iterator<Item = usize> {
        self.row_key_end..self.user_column_end
    }
}
```

Another useful feature of `StoreSchema` is that we ensure it always contains key columns, a timestamp column, and internal columns because we need them to perform merge, deduplication, and delete. Projection on `StoreSchema` only projects value columns.

## ProjectedSchema
To support arbitrary projection, we introduce the [ProjectedSchema](https://github.com/GreptimeTeam/greptimedb/blob/9fa871a3fad07f583dc1863a509414da393747f8/src/storage/src/schema/projected.rs#L106).
```rust
pub struct ProjectedSchema {
    projection: Option<Projection>,
    schema_to_read: StoreSchemaRef,
    projected_user_schema: SchemaRef,
}
```

We need to handle many cases while doing projection:
- The columns' order of table and region is different
- The projection can be in arbitrary order, e.g. `select usage_user, host from cpu` and `select host, usage_user from cpu` have different projection order
- We support `ALTER TABLE` so data files may have different schemas.

### Projection
Let's take an example to see how projection works. Suppose we want to select `ts`, `usage_system` from the `cpu` table.

```sql
CREATE TABLE cpu (
    ts TIMESTAMP,
    host STRING,
    usage_user DOUBLE,
    usage_system DOUBLE,
    datacenter STRING,
    TIME INDEX (ts),
    PRIMARY KEY(datacenter, host)) ENGINE=mito WITH(regions=1);

select ts, usage_system from cpu;
```

The query engine uses the projection `[0, 3]` to scan the table. However, columns in the region have a different order, so the table engine adjusts the projection to `2, 4`.
```json
{
    "user_schema":[
        "datacenter",
        "host",
        "ts",
        "usage_user",
        "usage_system"
    ],
}
```

As you can see, the output order is still `[ts, usage_system]`. This is the schema users can see after projection so we call it `projected user schema`.

But the storage engine also needs to read key columns, a timestamp column, and internal columns. So we maintain a `StoreSchema` after projection in the `ProjectedSchema`.

The `Projection` struct is a helper struct to help compute the projected user schema and store schema.

So we can construct the following `ProjectedSchema`:
```json
{
    "schema_to_read":{
        "schema":{
            "column_schemas":[
                "datacenter",
                "host",
                "ts",
                "usage_system",
                "__sequence",
                "__op_type"
            ],
            "time_index":2,
            "version":0
        },
        "row_key_end":3,
        "user_column_end":4
    },
    "projected_user_schema":{
        "column_schemas":[
            "ts",
            "usage_system"
        ],
        "time_index":0
    }
}
```

As you can see, `schema_to_read` doesn't contain the column `usage_user` that is not intended to be read (not in projection).

### ReadAdapter
As mentioned above, we can alter a table so the underlying files (SSTs) and memtables in the storage engine may have different schemas.

To simplify the logic of `ProjectedSchema`, we handle the difference between schemas before projection (constructing the `ProjectedSchema`). We introduce [ReadAdapter](https://github.com/GreptimeTeam/greptimedb/blob/9fa871a3fad07f583dc1863a509414da393747f8/src/storage/src/schema/compat.rs#L90) that adapts rows with different source schemas to the same expected schema.

So we can always use the current `RegionSchema` of the region to construct the `ProjectedSchema`, and then create a `ReadAdapter` for each memtable or SST.
```rust
#[derive(Debug)]
pub struct ReadAdapter {
    source_schema: StoreSchemaRef,
    dest_schema: ProjectedSchemaRef,
    indices_in_result: Vec<Option<usize>>,
    is_source_needed: Vec<bool>,
}
```

For each column required by `dest_schema`, `indices_in_result` stores the index of that column in the row read from the source memtable or SST. If the source row doesn't contain that column, the index is `None`.

The field `is_source_needed` stores whether a column in the source memtable or SST is needed.

Suppose we add a new column `usage_idle` to the table `cpu`.
```sql
ALTER TABLE cpu ADD COLUMN usage_idle DOUBLE;
```

The new `StoreSchema` becomes:
```json
{
    "schema":{
        "column_schemas":[
            "datacenter",
            "host",
            "ts",
            "usage_user",
            "usage_system",
            "usage_idle",
            "__sequence",
            "__op_type"
        ],
        "time_index":2,
        "version":1
    },
    "row_key_end":3,
    "user_column_end":6
}
```

Note that we bump the version of the schema to 1.

If we want to select `ts`, `usage_system`, and `usage_idle`. While reading from the old schema, the storage engine creates a `ReadAdapter` like this:
```json
{
    "source_schema":{
        "schema":{
            "column_schemas":[
                "datacenter",
                "host",
                "ts",
                "usage_user",
                "usage_system",
                "__sequence",
                "__op_type"
            ],
            "time_index":2,
            "version":0
        },
        "row_key_end":3,
        "user_column_end":5
    },
    "dest_schema":{
        "schema_to_read":{
            "schema":{
                "column_schemas":[
                    "datacenter",
                    "host",
                    "ts",
                    "usage_system",
                    "usage_idle",
                    "__sequence",
                    "__op_type"
                ],
                "time_index":2,
                "version":1
            },
            "row_key_end":3,
            "user_column_end":5
        },
        "projected_user_schema":{
            "column_schemas":[
                "ts",
                "usage_system",
                "usage_idle"
            ],
            "time_index":0
        }
    },
    "indices_in_result":[
        0,
        1,
        2,
        3,
        null,
        4,
        5
    ],
    "is_source_needed":[
        true,
        true,
        true,
        false,
        true,
        true,
        true
    ]
}
```

We don't need to read `usage_user` so `is_source_needed[3]` is false. The old schema doesn't have column `usage_idle` so `indices_in_result[4]` is `null` and the `ReadAdapter` needs to insert a null column to the output row so the output schema still contains `usage_idle`.

The figure below shows the relationship between `RegionSchema`, `StoreSchema`, `ProjectedSchema`, and `ReadAdapter`.

```text
                   ┌──────────────────────────────┐
                   │                              │
                   │    ┌────────────────────┐    │
                   │    │    store_schema    │    │
                   │    │                    │    │
                   │    │     StoreSchema    │    │
                   │    │      version 1     │    │
                   │    └────────────────────┘    │
                   │                              │
                   │    ┌────────────────────┐    │
                   │    │     user_schema    │    │
                   │    └────────────────────┘    │
                   │                              │
                   │         RegionSchema         │
                   │                              │
                   └──────────────┬───────────────┘
                                  │
                                  │
                                  │
                   ┌──────────────▼───────────────┐
                   │                              │
                   │ ┌──────────────────────────┐ │
                   │ │     schema_to_read       │ │
                   │ │                          │ │
                   │ │  StoreSchema (projected) │ │
                   │ │       version 1          │ │
                   │ └──────────────────────────┘ │
               ┌───┤                              ├───┐
               │   │ ┌──────────────────────────┐ │   │
               │   │ │  projected_user_schema   │ │   │
               │   │ └──────────────────────────┘ │   │
               │   │                              │   │
               │   │       ProjectedSchema        │   │
  dest schema  │   └──────────────────────────────┘   │   dest schema
               │                                      │
               │                                      │
        ┌──────▼───────┐                      ┌───────▼──────┐
        │              │                      │              │
        │  ReadAdapter │                      │  ReadAdapter │
        │              │                      │              │
        └──────▲───────┘                      └───────▲──────┘
               │                                      │
               │                                      │
source schema  │                                      │  source schema
               │                                      │
       ┌───────┴─────────┐                   ┌────────┴────────┐
       │                 │                   │                 │
       │ ┌─────────────┐ │                   │ ┌─────────────┐ │
       │ │             │ │                   │ │             │ │
       │ │ StoreSchema │ │                   │ │ StoreSchema │ │
       │ │             │ │                   │ │             │ │
       │ │  version 0  │ │                   │ │  version 1  │ │
       │ │             │ │                   │ │             │ │
       │ └─────────────┘ │                   │ └─────────────┘ │
       │                 │                   │                 │
       │      SST 0      │                   │      SST 1      │
       │                 │                   │                 │
       └─────────────────┘                   └─────────────────┘
```

# Conversion
This figure shows the conversion between schemas:
```text
              ┌─────────────┐     schema                      From             ┌─────────────┐
              │             ├──────────────────┐  ┌────────────────────────────►             │
              │  TableMeta  │                  │  │                            │  RawSchema  │
              │             │                  │  │  ┌─────────────────────────┤             │
              └─────────────┘                  │  │  │        TryFrom          └─────────────┘
                                               │  │  │
                                               │  │  │
                                               │  │  │
                                               │  │  │
                                               │  │  │
    ┌───────────────────┐                ┌─────▼──┴──▼──┐   arrow_schema()    ┌─────────────────┐
    │                   │                │              ├─────────────────────►                 │
    │  ColumnsMetadata  │          ┌─────►    Schema    │                     │   ArrowSchema   ├──┐
    │                   │          │     │              ◄─────────────────────┤                 │  │
    └────┬───────────▲──┘          │     └───▲───▲──────┘       TryFrom       └─────────────────┘  │
         │           │             │         │   │                                                 │
         │           │             │         │   └────────────────────────────────────────┐        │
         │           │             │         │                                            │        │
         │   columns │    user_schema()      │                                            │        │
         │           │             │         │ projected_user_schema()                 schema()    │
         │           │             │         │                                            │        │
         │       ┌───┴─────────────┴─┐       │                 ┌────────────────────┐     │        │
columns  │       │                   │       └─────────────────┤                    │     │        │  TryFrom
         │       │    RegionSchema   │                         │   ProjectedSchema  │     │        │
         │       │                   ├─────────────────────────►                    │     │        │
         │       └─────────────────┬─┘  ProjectedSchema::new() └──────────────────┬─┘     │        │
         │                         │                                              │       │        │
         │                         │                                              │       │        │
         │                         │                                              │       │        │
         │                         │                                              │       │        │
    ┌────▼────────────────────┐    │               store_schema()            ┌────▼───────┴──┐     │
    │                         │    └─────────────────────────────────────────►               │     │
    │   Vec<ColumnMetadata>   │                                              │  StoreSchema  ◄─────┘
    │                         ◄──────────────────────────────────────────────┤               │
    └─────────────────────────┘                     columns                  └───────────────┘
```
