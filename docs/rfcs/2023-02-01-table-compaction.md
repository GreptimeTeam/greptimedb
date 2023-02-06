---
Feature Name: "table-compaction"
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/930
Date: 2023-02-01
Author: "Lei, HUANG <mrsatangel@gmail.com>"
---

# Table Compaction

--- 

## Background

GreptimeDB uses an LSM-tree based storage engine that flushes memtables to SSTs for persistence. 
But currently it only supports level 0. SST files in level 0 does not guarantee to contain only rows with disjoint time ranges. 
That is to say, different SST files in level 0 may contain overlapped timestamps. 
The consequence is, in order to retrieve rows in some time range, all files need to be scanned, which brings a lot of IO overhead.

Also, just like other LSMT engines, delete/update to existing primary keys are converted to new rows with delete/update mark and appended to SSTs on flushing. 
We need to merge the operations to same primary keys so that we don't have to go through all SST files to find the final state of these primary keys.  

## Goal

Implement a compaction framework to: 
- maintain SSTs in timestamp order to accelerate queries with timestamp condition;
- merge rows with same primary key;
- purge expired SSTs;
- accommodate other tasks like data rollup/indexing.


## Overview

Table compaction involves following components:
- Compaction scheduler: run compaction tasks, limit the consumed resources;
- Compaction strategy: find the SSTs to compact and determine the output files of compaction.
- Compaction task: read the rows from input SSTs and write to the output files.

## Implementation

### Compaction scheduler

`CompactionScheduler` is an executor that continuously polls and executes compaction request from a task queue. 

```rust
#[async_trait]
pub trait CompactionScheduler {
    /// Schedules a compaction task.
    async fn schedule(&self, task: CompactionRequest) -> Result<()>;

    /// Stops compaction scheduler.
    async fn stop(&self) -> Result<()>;
}
```



### Compaction triggering

Currently, we can check whether to compact tables when memtable is flushed to SST.

https://github.com/GreptimeTeam/greptimedb/blob/4015dd80752e1e6aaa3d7cacc3203cb67ed9be6d/src/storage/src/flush.rs#L245


### Compaction strategy

`CompactionStrategy` defines how to pick SSTs in all levels for compaction.   

```rust
pub trait CompactionStrategy {
    fn pick(
        &self,
        ctx: CompactionContext,
        levels: &LevelMetas,
    ) -> Result<CompactionTask>;
}
```

The most suitable compaction strategy for time-series scenario would be 
a hybrid strategy that combines time window compaction with size-tired compaction, just like [Cassandra](https://cassandra.apache.org/doc/latest/cassandra/operating/compaction/twcs.html) and [ScyllaDB](https://docs.scylladb.com/stable/architecture/compaction/compaction-strategies.html#time-window-compaction-strategy-twcs) does.

We can first group SSTs in level n into buckets according to some predefined time window. Within that window, 
SSTs are compacted in a size-tired manner (find SSTs with similar size and compact them to level n+1). 
SSTs from different time windows are neven compacted together.
That strategy guarantees SSTs in each level are mainly sorted in timestamp order which boosts queries with 
explicit timestamp condition, while size-tired compaction minimizes the impact to foreground writes. 

### Alternatives

Currently, GreptimeDB's storage engine [only support two levels](https://github.com/GreptimeTeam/greptimedb/blob/43aefc5d74dfa73b7819cae77b7eb546d8534a41/src/storage/src/sst.rs#L32).
For level 0, we can start with a simple time-window based leveled compaction, which reads from all SSTs in level 0, 
align them to time windows with a fixed duration, merge them with SSTs in level 1 within the same time window 
to ensure there is only one sorted run in level 1.