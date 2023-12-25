---
Feature Name: Enclose Column Id
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/2982
Date: 2023-12-22
Author: "Ruihang Xia <waynestxia@gmail.com>"
---

# Summary
This RFC proposes to enclose the usage of `ColumnId` into the region engine only.

# Motivation
`ColumnId` is an identifier for columns. It's assigned by meta server, stored in `TableInfo` and `RegionMetadata` and used in region engine to distinguish columns.

At present, Both Frontend, Datanode and Metasrv are aware of `ColumnId` but it's only used in region engine. Thus this RFC proposes to remove it from Frontend (mainly used in `TableInfo`) and Metasrv.

# Details

`ColumnId` is used widely on both read and write paths. Removing it from Frontend and Metasrv implies several things:

- A column may have different column id in different regions.
- A column is identified by its name in all components.
- Column order in the region engine is not restricted, i.e., no need to be in the same order with table info.

The first thing doesn't matter IMO. This concept doesn't exist anymore outside of region server, and each region is autonomous and independent -- the only guarantee it should hold is those columns exist. But if we consider region repartition, where the SST file would be re-assign to different regions, things would become a bit more complicated. A possible solution is store the relation between name and ColumnId in the manifest, but it's out of the scope of this RFC. We can likely give a workaround by introducing a indirection mapping layer of different version of partitions.

And more importantly, we can still assume columns have the same column ids across regions. We have procedure to maintain consistency between regions and the region engine should ensure alterations are idempotent. So it is possible that region repartition doesn't need to consider column ids or other region metadata in the future.

Users write and query column by their names, not by ColumnId or something else. The second point also means to change the column reference in ScanRequest from index to name. This change can hugely alleviate the misuse of the column index, which has given us many surprises.

And for the last one, column order only matters in table info. This order is used in user-faced table structure operation, like add column, describe column or as the default order of INSERT clause. None of them is connected with the order in storage.

# Drawback
Firstly, this is a breaking change. Delivering this change requires a full upgrade of the cluster. Secondly, this change may introduce some performance regression. For example, we have to pass the full table name in the `ScanRequest` instead of the `ColumnId`. But this influence is very limited, since the column index is only used in the region engine.

# Alternatives

There are two alternatives from the perspective of "what can be used as the column identifier":

- Index of column to the table schema
- `ColumnId` of that column

The first one is what we are using now. By choosing this way, it's required to keep the column order in the region engine the same as the table info. This is not hard to achieve, but it's a bit annoying. And things become tricky when there is internal column or different schemas like those stored in file format. And this is the initial purpose of this RFC, which is trying to decouple the table schema and region schema.

The second one, in other hand, requires the `ColumnId` should be identical in all regions and `TableInfo`. It has the same drawback with the previous alternative, that the `TableInfo` and `RegionMetadata` are tighted together. Another point is that the `ColumnId` is assigned by the Metasrv, who doesn't need it but have to maintain it. And this also limits the functionality of `ColumnId`, by taking the ability of assigning it from concrete region engine.
