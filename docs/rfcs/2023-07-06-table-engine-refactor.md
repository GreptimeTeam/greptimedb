---
Feature Name: table-engine-refactor
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/1869
Date: 2023-07-06
Author: "Yingwen <realevenyag@gmail.com>"
---

Refactor Table Engine
----------------------

# Summary
Refactor table engines to address several historical tech debts.

# Motivation
Both `Frontend` and `Datanode` have to deal with multiple regions in a table. This results in code duplication and additional burden to the `Datanode`.

Before:


```mermaid
graph TB

subgraph Frontend["Frontend"]
    subgraph MyTable
        A("region 0, 2 -> Datanode0")
        B("region 1, 3 -> Datanode1")
    end
end

MyTable --> MetaSrv
MetaSrv --> ETCD

MyTable-->TableEngine0
MyTable-->TableEngine1

subgraph Datanode0
    Procedure0("procedure")
    TableEngine0("table engine")
    region0
    region2
    mytable0("my_table")

    Procedure0-->mytable0
    TableEngine0-->mytable0
    mytable0-->region0
    mytable0-->region2
end


subgraph Datanode1
    Procedure1("procedure")
    TableEngine1("table engine")
    region1
    region3
    mytable1("my_table")

    Procedure1-->mytable1
    TableEngine1-->mytable1
    mytable1-->region1
    mytable1-->region3
end


subgraph manifest["table manifest"]
    M0("my_table")
    M1("regions: [0, 1, 2, 3]")
end

mytable1-->manifest
mytable0-->manifest

RegionManifest0("region manifest 0")
RegionManifest1("region manifest 1")
RegionManifest2("region manifest 2")
RegionManifest3("region manifest 3")
region0-->RegionManifest0
region1-->RegionManifest1
region2-->RegionManifest2
region3-->RegionManifest3
```

`Datanodes` can update the same manifest file for a table as regions are assigned to different nodes in the cluster. We also have to run procedures on `Datanode` to ensure the table manifest is consistent with region manifests. "Table" in a `Datanode` is a subset of the table's regions. The `Datanode` is much closer to `RegionServer` in `HBase` which only deals with regions.

In cluster mode, we store table metadata in etcd and table manifest. The table manifest becomes redundant. We can remove the table manifest if we refactor the table engines to region engines that only care about regions. What's more, we don't need to run those procedures on `Datanode`.

After:
```mermaid
graph TB

subgraph Frontend["Frontend"]
    direction LR
    subgraph MyTable
        A("region 0, 2 -> Datanode0")
        B("region 1, 3 -> Datanode1")
    end
end

MyTable --> MetaSrv
MetaSrv --> ETCD

MyTable-->RegionEngine
MyTable-->RegionEngine1

subgraph Datanode0
    RegionEngine("region engine")
    region0
    region2
    RegionEngine-->region0
    RegionEngine-->region2
end


subgraph Datanode1
    RegionEngine1("region engine")
    region1
    region3
    RegionEngine1-->region1
    RegionEngine1-->region3
end

RegionManifest0("region manifest 0")
RegionManifest1("region manifest 1")
RegionManifest2("region manifest 2")
RegionManifest3("region manifest 3")
region0-->RegionManifest0
region1-->RegionManifest1
region2-->RegionManifest2
region3-->RegionManifest3
```
This RFC proposes to refactor table engines into region engines as a first step to make the `Datanode` acts like a `RegionServer`.


# Details
## Overview

We plan to refactor the `TableEngine` trait into `RegionEngine` gradually. This RFC focuses on the `mito` engine as it is the default table engine and the most complicated engine.

Currently, we built `MitoEngine` upon `StorageEngine` that manages regions of the `mito` engine. Since `MitoEngine` becomes a region engine, we could combine `StorageEngine` with `MitoEngine` to simplify our code structure.

The chart below shows the overall architecture of the `MitoEngine`.

```mermaid
classDiagram
class MitoEngine~LogStore~ {
    -WorkerGroup workers
}
class MitoRegion {
    +VersionControlRef version_control
    -RegionId region_id
    -String manifest_dir
    -AtomicI64 last_flush_millis
    +region_id() RegionId
    +scan() ChunkReaderImpl
}
class RegionMap {
    -HashMap&lt;RegionId, MitoRegionRef&gt; regions
}
class ChunkReaderImpl

class WorkerGroup {
    -Vec~RegionWorker~ workers
}
class RegionWorker {
    -RegionMap regions
    -Sender sender
    -JoinHandle handle
}
class RegionWorkerThread~LogStore~ {
    -RegionMap regions
    -Receiver receiver
    -Wal~LogStore~ wal
    -ObjectStore object_store
    -MemtableBuilderRef memtable_builder
    -FlushSchedulerRef~LogStore~ flush_scheduler
    -FlushStrategy flush_strategy
    -CompactionSchedulerRef~LogStore~ compaction_scheduler
    -FilePurgerRef file_purger
}
class Wal~LogStore~ {
    -LogStore log_store
}
class MitoConfig

MitoEngine~LogStore~ o-- MitoConfig
MitoEngine~LogStore~ o-- MitoRegion
MitoEngine~LogStore~ o-- WorkerGroup
MitoRegion o-- VersionControl
MitoRegion -- ChunkReaderImpl
WorkerGroup o-- RegionWorker
RegionWorker o-- RegionMap
RegionWorker -- RegionWorkerThread~LogStore~
RegionWorkerThread~LogStore~ o-- RegionMap
RegionWorkerThread~LogStore~ o-- Wal~LogStore~
```

We replace the `RegionWriter` with `RegionWorker` to process write requests and DDL requests.


## Metadata
We also merge region's metadata with table's metadata. It should make metadata much easier to maintain.
```mermaid
classDiagram
class VersionControl {
    -CowCell~Version~ version
    -AtomicU64 committed_sequence
}
class Version {
    -RegionMetadataRef metadata
    -MemtableVersionRef memtables
    -LevelMetasRef ssts
    -SequenceNumber flushed_sequence
    -ManifestVersion manifest_version
}
class MemtableVersion {
    -MemtableRef mutable
    -Vec~MemtableRef~ immutables
    +mutable_memtable() MemtableRef
    +immutable_memtables() &[MemtableRef]
    +freeze_mutable(MemtableRef new_mutable) MemtableVersion
}
class LevelMetas {
    -LevelMetaVec levels
    -AccessLayerRef sst_layer
    -FilePurgerRef file_purger
    -Option~i64~ compaction_time_window
}
class LevelMeta {
    -Level level
    -HashMap&lt;FileId, FileHandle&gt; files
}
class FileHandle {
    -FileMeta meta
    -bool compacting
    -AtomicBool deleted
    -AccessLayerRef sst_layer
    -FilePurgerRef file_purger
}
class FileMeta {
    +RegionId region_id
    +FileId file_id
    +Option&lt;Timestamp, Timestamp&gt; time_range
    +Level level
    +u64 file_size
}

VersionControl o-- Version
Version o-- RegionMetadata
Version o-- MemtableVersion
Version o-- LevelMetas
LevelMetas o-- LevelMeta
LevelMeta o-- FileHandle
FileHandle o-- FileMeta

class RegionMetadata {
    +RegionId region_id
    +VersionNumber version
    +SchemaRef table_schema
    +Vec~usize~ primary_key_indices
    +Vec~usize~ value_indices
    +ColumnId next_column_id
    +TableOptions region_options
    +DateTime~Utc~ created_on
    +RegionSchemaRef region_schema
}
class RegionSchema {
    -SchemaRef user_schema
    -StoreSchemaRef store_schema
    -ColumnsMetadataRef columns
}
class Schema
class StoreSchema {
    -Vec~ColumnMetadata~ columns
    -SchemaRef schema
    -usize row_key_end
    -usize user_column_end
}
class ColumnsMetadata {
    -Vec~ColumnMetadata~ columns
    -HashMap&lt;String, usize&gt; name_to_col_index
    -usize row_key_end
    -usize timestamp_key_index
    -usize user_column_end
}
class ColumnMetadata

RegionMetadata o-- RegionSchema
RegionMetadata o-- Schema
RegionSchema o-- StoreSchema
RegionSchema o-- Schema
RegionSchema o-- ColumnsMetadata
StoreSchema o-- ColumnsMetadata
StoreSchema o-- Schema
StoreSchema o-- ColumnMetadata
ColumnsMetadata o-- ColumnMetadata
```

# Drawback
This is a breaking change.

# Future Work
- Rename `TableEngine` to `RegionEngine`
- Simplify schema relationship in the `mito` engine
- Refactor the `Datanode` into a `RegionServer`.
