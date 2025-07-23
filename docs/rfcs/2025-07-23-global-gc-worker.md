---
Feature Name: "global-gc-worker"
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/6571
Date: 2025-07-23
Author: "discord9 <discord9@163.com>"
---

# Global GC Worker

## Summary

This RFC proposes a background worker for globally garbage collecting stale files that are no longer in use by any component of the system.

## Motivation

With the introduction of features like table repartitioning, a significant number of Parquet files can become obsolete. Additionally, failures during manifest updates can lead to orphaned files that are never referenced. A periodic garbage collection mechanism is required to reclaim storage space by removing these unused files.

## Details

### Overview

The Global GC Worker will run periodically. In each run, it scans the table manifests to identify all currently used file IDs. Subsequently, it traverses the storage directory and deletes files that are not in the set of used file IDs.

This design prioritizes simplicity of implementation, which implies that correctness might not be guaranteed under certain rare edge cases. In such cases, the system might delete files that are still being read by long-running queries on read replicas, causing those queries to fail. However, this is considered an acceptable trade-off for the simplicity of the implementation.

### Terminology

- **Unused File**: A file that exists in the storage directory but has never been recorded in any manifest. This can occur, for example, when an SST file is successfully written, but the subsequent manifest update fails.
- **Obsolete File**: A file that was once recorded in a manifest but has since been explicitly marked for removal, for instance, after a repartition or compaction operation.

### GC Worker Process

The Global GC Worker operates with a long interval, ranging from tens of minutes to several hours, and can also be triggered manually. To execute a GC cycle, `metasrv` will select a `datanode` with a low current workload to run the worker. The execution on a `datanode` is preferred to avoid the overhead of passing object storage configurations to the `metasrv`.

The process is as follows:

1.  A separate GC task is initiated for each table. If any compaction or other action that should prevent GC is happening on this table, the GC worker will skip it and wait for the next cycle.
2.  The worker reads the table's manifest to get a list of all obsolete files (Note: for now, it only checks regions that have undergone repartition).
3.  It performs a streaming scan of the object storage directory to identify all unused files. Caching might be considered, but its benefit is limited given the long execution interval.

#### Handling Obsolete Files

An obsolete file is permanently deleted if the time since its removal from the manifest (its obsolescence timestamp) exceeds a configurable threshold.

#### Handling Unused Files

An unused file is permanently deleted if its creation time exceeds a configurable threshold.

A list of recently deleted files can be maintained for debugging purposes.

### Ensuring Read Consistency

To prevent the GC worker from deleting files that are actively being used by long-running analytical queries on read replicas, a protection mechanism is introduced. When a read replica initiates a query (or after a query runs for a certain duration), it will write a temporary, "fake" manifest. This manifest will reference all the files required for the query, along with the manifest version, effectively placing a hold on them and preventing their deletion by the GC worker.

## Drawbacks

- The long interval between GC cycles means that frequent repartitioning could lead to a temporary accumulation of obsolete files.
- There is a small but non-zero probability of incorrect deletion. For example, if a compaction operation takes an unusually long time, some of its newly created SST files might be uploaded while one SST is still pending. The uploaded SSTs will be treated as "Unused files" because they don't show up in the manifest, and if the compaction takes longer than the deletion threshold to update the manifest, it might mistakenly identify the uploaded SSTs as unused and delete them. However, the likelihood of such an event is extremely low and would require a combination of a very long-running query on a read replica and an exceptionally long compaction process.

## Alternatives

A more complex alternative implementation would require the GC worker to have greater awareness of the table state and the cluster of read replicas. This would ensure that a file is truly unused before deletion, but at the cost of significantly higher implementation complexity.

### Alternative GC Process Details

1.  **Handling Leaked Files from Failed Manifest Updates**: These are files that never appeared in the manifest. Their growth rate is slow. The system could wait for a much longer time to confirm that any associated flush/compaction has completed or failed before deleting them. Alternatively, they could be marked with metrics instead of being deleted immediately.

2.  **Handling Files from Repartitioning**: These files are explicitly removed from the manifest via a `RegionEdit` after a repartition completes. They can be deleted quickly. However, if a read replica is still accessing these files, a synchronization mechanism is needed. The read replica would need to expose an interface allowing the GC worker to query whether specific files are in use. This would allow the worker to extend the "lease" on these files instead of deleting them.

### Detailed Alternative Workflow

1.  After the distributed cluster starts, `metasrv` designates a `datanode` to start the Global GC Worker. The `datanode`'s heartbeat can include the status of the worker.

2.  The Global GC Worker runs periodically (with a configurable interval). Its process is as follows:
    - For each region in object storage:
        - Read the manifest to get all active file IDs and all obsolete file IDs.
        - Scan the file list in storage. Files present in storage but marked as removed in the manifest are labeled as `ObsoleteFile`. Files present in storage but never in any manifest are labeled as `UnusedFile`.
    - For new `ObsoleteFile`s, grant a lease (e.g., 30 seconds). When enough leased files expire or after a certain time, query the read replicas to check if any of these files are still in use. If not, delete them. If they are in use, renew the lease (e.g., double the lease time, up to a max of 5 minutes) to accommodate long-running queries.
    - For new `UnusedFile`s, two scenarios must be distinguished:
        1.  A compaction/flush is in progress. The SST is created, but the manifest is not yet updated. The GC worker should be paused for this table until the operation completes. An interface would be needed to signal whether GC is permissible on a table.
        2.  A manifest update has failed, leading to a permanently leaked SST. In this case, the `UnusedFile` can be safely deleted during the next GC run.

In summary, an `UnusedFile` can be deleted when the GC worker is permitted to run on its corresponding table.

## Comparison

| Aspect | Primary Proposal (Simplicity-First) | Alternative Proposal (Safety-First) |
| :--- | :--- | :--- |
| **Implementation Complexity** | **Low**. The core logic relies on a fixed time threshold, avoiding complex state synchronization between components. | **High**. Requires implementing a lease mechanism, a status query interface for communication with read replicas, and table-level signals to pause GC, leading to tighter component coupling. |
| **Reliability** | **Lower**. There is a very low probability of incorrectly deleting files being read by long-running queries, which would cause those queries to fail. This risk is accepted for the sake of simplicity. | **High**. The lease and active query mechanism ensures a file is no longer in use by any component before deletion, significantly mitigating the risk of accidental deletion. |
| **Performance Overhead** | **Low**. The GC Worker runs independently and does not impose direct polling or request overhead on other components like read replicas. | **Medium**. The GC Worker needs to periodically communicate with read replicas to check file leases, introducing additional network and processing overhead. |
| **Impact on Other Components** | **Minor**. Primarily relies on read replicas to protect themselves by writing a "fake manifest" during long-running queries. | **Significant**. Requires read replicas to expose an interface for querying file usage status and demands that operations like compaction can signal to pause GC on specific tables. |
| **Deletion Strategy** | **Time-Based**. Both unused and obsolete files are deleted based on a configurable time threshold. | **State- and Lease-Based**. Employs a more granular strategy: sets leases for obsolete files and confirms their status upon expiration; distinguishes between ongoing operations and genuine leaks for unused files. |

## Unresolved Questions

- The exact conditions for a read replica to write a "fake" manifest need further discussion.
- The strategy for handling leaked files in the primary proposal (delete after a threshold) versus the alternative (mark with metrics) needs to be decided.
