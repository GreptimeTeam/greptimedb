---
Feature Name: Update Metadata in single transaction
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/1715
Date: 2023-08-13
Author: "Feng Yangsen <fengys1996@gmail.com>, Xu Wenkang <wenymedia@gmail.com>"
---

# Summary
Update Metadata in single transaction.

# Motivation
Currently, multiple transactions are involved during the procedure. This implementation is inefficient, and it's hard to make data consistent. Therefore, We can update multiple metadata in a single transaction.

# Details 
Now we have the following table metadata keys:

**TableInfo** 
```rust
// __table_info/{table_id}
pub struct TableInfoKey {
    table_id: TableId,
}

pub struct TableInfoValue {
    pub table_info: RawTableInfo,
    version: u64,
}
```

**TableRoute** 
```rust
// __table_route/{table_id}
pub struct NextTableRouteKey {
    table_id: TableId,
}

pub struct TableRoute {
    pub region_routes: Vec<RegionRoute>,
}
```
**DatanodeTable**
```rust
// __table_route/{datanode_id}/{table_id}
pub struct DatanodeTableKey {
    datanode_id: DatanodeId,
    table_id: TableId,
}

pub struct DatanodeTableValue {
    pub table_id: TableId,
    pub regions: Vec<RegionNumber>,
    version: u64,
}
```

**TableNameKey**
```rust
// __table_name/{CatalogName}/{SchemaName}/{TableName}
pub struct TableNameKey<'a> {
    pub catalog: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
}

pub struct TableNameValue {
    table_id: TableId,
}
```

These table metadata only updates in the following operations.

## Region Failover
It needs to update `TableRoute` key and `DatanodeTable` keys. If the `TableRoute` equals the Snapshot of `TableRoute` submitting the Failover task, then we can safely update these keys.

After submitting Failover tasks to acquire locks for execution, the `TableRoute` may be updated by another task. After acquiring the lock, we can get the latest `TableRoute` again and then execute it if needed.

## Create Table DDL
Creates all of the above keys. `TableRoute`, `TableInfo`, should be empty.

The **TableNameKey**'s lock will be held by the procedure framework.
## Drop Table DDL

`TableInfoKey` and `NextTableRouteKey` will be added with  `__removed-` prefix, and the other above keys will be deleted.  The transaction will not compare any keys.
## Alter Table DDL

1. Rename table, updates `TableInfo` and `TableName`. Compares `TableInfo`, and the new `TableNameKey` should be empty, and TableInfo should equal the Snapshot when submitting DDL.

The old and new **TableNameKey**'s lock will be held by the procedure framework.

2. Alter table, updates `TableInfo`. `TableInfo` should equal the Snapshot when submitting DDL.
