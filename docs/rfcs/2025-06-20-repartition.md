---
Feature Name: Repartition
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/6558
Date: 2025-06-20
Author: "Ruihang Xia <waynestxia@gmail.com>"
---

# Summary

This RFC proposes a method for repartitioning a table, to adjust the partition rule and data distribution.

# Motivation

With time passing, the data distribution and skew pattern of a table might change. We need a way to repartition the table to suit the new pattern.

# Details

Here is a rough workflow diagram of the entire repartition process, each step is described in detail below.

```mermaid
sequenceDiagram
    participant Frontend
    participant Metasrv
    participant Datanodes
    participant Region0 as Region 0
    
    Frontend->>Frontend: Process request, validation etc.
    Frontend->>Metasrv: Submit procedure
    Metasrv->>Metasrv: Compute diff and generate migration plan
    Metasrv->>Metasrv: Allocate necessary region resources (with Paas)
    Metasrv->>Datanodes: Stop compaction and snapshot
    rect rgb(255, 225, 225)
        note over Frontend, Region0: No Ingestion Period
        Metasrv->>Frontend: Stop processing write requests
        Metasrv->>Metasrv: Update metadata
        Metasrv->>Frontend: Start processing read requests
    end
    Metasrv->>Datanodes: Update region rule, stage version changes from now on
    Region0->>Region0: Compute new manifests for all regions
    Region0->>Datanodes: Submit manifest changes
    Metasrv->>Metasrv: Checkpoint publish after all regions acknowledge apply
    Metasrv->>Datanodes: Publish staged changes and resume compaction and snapshot

    note over Frontend, Datanodes: Reload Cache
    Metasrv->>Metasrv: Release resources (with Paas)
    Metasrv->>Metasrv: Schedule optional compaction (to remote compactor)
```

## Preprocessing

This phase is for static analysis of the new partition rule. The server can know whether the repartitioning is possible, how to do the repartitioning, and how much resources are needed.

In theory, the input and output partition rules for repartitioning can be completely unrelated. But in practice, to avoid a very large change set, we'll only allow two simple kinds of change. One splits one region into two regions (region split) and another merges two regions into one (region merge).

After validating the new partition rule using the same validation logic as table creation, we compute the difference between the old and new partition rules. The resulting diff may contain several independent groups of changes. During subsequent processing, each group of changes can be handled independently and can succeed or fail without affecting other groups or creating non-idempotently retryable scenarios.

Next, we generate a repartition plan for each group of changes. Each plan contains this information for all regions involved in that particular plan. And one target region will only be referenced by a single plan.

With those plans, we can determine the resource requirements for the repartition operation, where resources here primarily refer to Regions. Metasrv will coordinate with PaaS layer to pre-allocate the necessary regions at this stage. These new regions start completely empty, and their metadata and manifests will be populated during subsequent modification steps.

## Data Processing

This phase is primarily for region's change, including region's metadata (route table and the corresponding rule) and manifest.

Once we start processing one plan through a procedure, we'll first stop the region's compaction and snapshot. This is to avoid any states being removed due to compaction (which may removes old SST files) and snapshot (which may removes old manifest files).

Metasrv will trying to update the metadata of partition, or the region route table (related to `PartitionRuleManager`). This step is in the "no ingestion" scope, so no new data will be ingested. Since this won't take much time, the affection to the cluster is minimized. Metasrv will also update the region rule to corresponding regions on Datanodes. 

Every regions and all the ingestion requests to the region server will have a version of region rule, to identify under which rule the request is processed. The version can be something like `hash(region_rule)`. Once the region rule on region server is updated, all ingestion request with old rule will be rejected, and all requests with new rule will be accepted but not visible. They can still be flushed to persisted storage, but their version change (new manifest) will be staged.

Then region 0 (or let metasrv to pick any operational region) will compute the new manifests for all target regions. This step is done by first reading all old manifests, and remapping the files with new partition rule, to get the content of new manifests. Notice this step only handles the manifests before region rule change on region server, and won't touch those staged manifests, as they are already with the new rule.

Region 0 submits the new manifests to the target regions via `RegionEdit`. Once a region applies its manifest, it enters `PendingPublish`. It can still accept writes under the new rule and flush them while other regions in the group apply their manifests. These flushes are recorded in the same staging manifest. The old manifest remains visible to readers, and compaction and snapshot remain stopped.

Once all regions in the group reach `PendingPublish`, metasrv records the decision to publish before asking any region to make its changes visible. Each region publishes the applied manifest together with its staged writes and can then resume compaction and snapshot.

## Postprocessing

After the main processing is done, we can do some extra postprocessing to reduce the performance impact of repartition. Including reloading caches in frontend's route table, metasrv's kv cache and datanode's read/write/page cache etc.

We can also schedule an optional compaction to reorganize all the data file under the new partition rule to reduce potential fragmentation or read amplification.

## Procedure

Here describe the repartition procedure step by step:

- <on frontend> Validating repartition request
- <on frontend> Initialize the repartition procedure
- Calculate rule diff and repartition plan group
- Allocate necessary new regions
- Lock the table key
- For each repartition subprocedure
  - Stop compaction and snapshot
  - Forbid new ingestion requests, update metadata, allow ingestion requests.
  - Update region rule to regions
  - Pick one region to calculate new manifest for all regions in this repartition group
  - Let that region to apply new manifest to each region via `RegionEdit`
  - If apply fails after retries, record the decision to roll back the group and recover its staged writes as described below.
  - If all succeeded, record the decision to publish the group and make its staged changes visible.
  - Return result
- Collect results from subprocedure.
  - For groups that rolled back, keep their old partition rules.
  - For those who succeeded, collect and merge their rule diff
- Unlock the table key
- Report the result to user.
- <in background> Reload cache
- <in background> Maybe trigger a special compaction

In addition of sequential step, rollback is also an important part of this procedure. There are three steps can be rolled back when unrecoverable failure occurs.

If the metadata update is not committed, we can overwrite the metadata to previous version. This step is scoped in the "no ingestion" period, so no new data will be ingested and the status of both datanode and metasrv will be consistent.

If apply fails after the new rule starts accepting writes, metasrv records the rollback decision and fences the union of the old and new regions in the group. This includes a reused region only once. The fence is enforced at the datanode so that requests sent with a cached route cannot enter after the fence. A region that never entered staging still has to acknowledge the fence. It contributes no staged writes.

Each region drains admitted writes and flushes the remaining staged memtables before acknowledging the fence. It seals the list of its own flush edits for this group, including flushes in `PendingPublish`. Imported files are excluded. An empty sealed list means that the region has no files to remap. A missing reply or an unreadable list must be retried. Metasrv waits for every seal before asking the central region to remap these files under the old partition rules.

Each old region keeps its original manifest and applies its share of the remapped files as one edit. The edit records the group ID and rollback operation together with the file changes, including when there are no files to add. A retry can then acknowledge the same edit without importing files twice. A reused region discards its unpublished forward changes. A newly allocated region stays fenced until its files are safe to release.

After all old regions acknowledge the edit, metasrv restores the group's old routes under the table lock and invalidates the route cache. It restores any table-wide partition metadata only if the surviving groups no longer require it. Before releasing an allocated region, metasrv must preserve any SST and index files still referenced by an old region. The rollback can use copies owned by the old regions, or the existing cross-region file references with the corresponding source-to-destination GC mapping installed before cleanup. Old regions resume writes under the old rule. A failed metadata update, reopen or cleanup is retried from the same rollback decision.

### Procedure recovery

Before dispatching groups, the procedure manager persists the child IDs and enough initial state to start each child. If metasrv loses leadership during dispatch, the new leader restores the parent and its children under the same IDs. It starts any child that had not yet started and resumes each running child from its checkpoint. The parent waits for these children. If a child finished before the parent collected its result, its result remains available until the parent has persisted the outcome.

Each group owns its publish or rollback decision. After every target region acknowledges apply, the group checkpoints the publish step before sending the first publish request. If apply fails permanently before this checkpoint, the group checkpoints rollback before fencing any region. After a crash it continues the recorded step. If an apply reply was lost before either decision, the group repeats apply for the same group ID. A region already in `PendingPublish` acknowledges the request and keeps any subsequent flushes.

A group that restarts during rollback repeats the fence and uses the sealed manifests to finish restoring the old regions. It reports failure to the parent only after its old routes and regions are usable again. The parent collects all group results even if one fails. For example, if one split publishes and another rolls back, the DDL reports failure, the first split keeps its new routes, and the second keeps its old routes. If the parent itself restarts during cleanup or rollback, it uses these same outcomes before changing table metadata or releasing regions.

On region open, the datanode first rebuilds the published manifest from its checkpoint and deltas. For an active group it also reconstructs the separate staging manifest before WAL replay. Entering staging must durably identify the group and its base manifest before accepting staged writes. Restore the prepared manifest if apply completed, the ordered local flush edits and their WAL watermark, and any seal. A published or rollback edit already present in the main manifest takes precedence over leftover staging records. Keep staged files out of the read version. A recovered group can then repeat apply or request the sealed manifest from the current datanode leader. If required staging records cannot be read, retry recovery before serving the region.

A datanode recovers the active group, its phase and any fence before serving writes or scheduling compaction. Region commands carry the group ID and check the persisted phase under the region worker. Delayed forward commands cannot reopen a fenced region. Persist the sealed flush list before acknowledging a seal. Persist the terminal group and operation in the same manifest update that installs the files before acknowledging publish or rollback apply. After a lost reply, the same command returns success if that operation is already recorded. Reopening a region must reconcile the staging flush watermark with WAL replay so that acknowledged writes after the last staging flush are included exactly once. Writes admitted without a WAL require a durable flush before acknowledgement to provide this guarantee.

## Region rule validation and diff calculation

In the current codebase, the rule checker is not complete. It can't check uniqueness and completeness of the rule. This RFC also propose a new way to validate the rule.

The proposed validation way is based on a check-point system, which first generates a group of check-points from the rule, and then check if all the point is covered and only covered by one rule.

All the partition rule expressionis limited to be the form of `<column> <operator> <value>`, and the operator is limited to be comparison operators. Those expressions are allowed to be nested with `AND` and `OR` operators. Based on this, we can first extract all the unique values on each column, adding and subtracting a little epsilon to cover its left and right boundary.

Since we accept integer, float and string as the value type, compute on them directly is not convenient. So we'll first normalize them to a common type and only need to preserve the relative partial ordering. This also avoids the problem of "what is next/previous value" of string and "what's a good precision" for float.

After normalization, we get a set of scatter points for each column. Then we can generate a set of check-points by combining all the scatter points like building a cartesian product. This might bring a large number of check-points, so we can do an prune optimization to remove some of them by merging some of the expression zones. Those expressions who have identical N-1 edge sub-expressions with one adjacent edge can be merged together. This prune check is with a time complexity of O(N * M * log(M)), where N is the number of active dimensions and M is the number of expression zones. Diff calculation is also done by finding different expression zones between the old and new rule set, and check if we can transform one to another by merging some of the expression zones.

The step to validate the check-points set against expressions can be treated as a tiny expression of `PhysicalExpr`. This evaluation will give a boolean matrix of K*M shape, where K is the number of check-points. We then check in each row of the matrix, if there is one and only one true value.

## Compute and use new manifest

We can generate a new set of manifest file based on old manifest and two versions of rule. From abvoe rule processing part, we can tell how a new rule & region is from previous one. So a simple way to get the new manifest is also apply the step of change to manifest files. E.g., if region A is from region B and C, we simply combine all file IDs from B and C to generate the content of A.

If necessary, we can do this better by involving some metadata related to data, like min-max statistics of each file, and pre-evaluate over min-max to filter out unneeded files when generating new manifest.

The way to use new manifest needs one more extra step based on the current implementation. We'll need to record either in manifest or in file metadata, of what rule is used when generating (flush or compaction) a SST file. Then in every single read request, we need to append the current region rule as predicate to the read request, to ensure no data belong to other regions will be read. We can use the stored region rule to reduce the number of new predicates to apply, by removing the identical predicate between the current region rule and the stored region rule. So ideally in a table that has not been repartitioned recently, the overhead of checking region rule is minimal.

## Pre-required tasks

In above steps, we assume some functionalities are implemented. Here list them with where they are used and how to implement them.

### Cross-region read

The current data directory structure is `{table_id}/{region_id}/[data/metadata]/{file_id}`, every region can only access files under their own directory. After repartition, data file may be placed in other previous old regions. So we need to support cross-region read. This new access method allows region to access any file under the same table. Related tracking issue is <https://github.com/GreptimeTeam/greptimedb/issues/6409>.

### Global GC worker

This is to simplify state management of data files. As one file may be referenced in multiple manifests, or no manifest at all. After this, every region and the repartition process only need to care about generateing and using new files, without tracking whether a file should be deleted or not. Leaving the deletion to the global GC worker. This worker basically works by counting reference from manifest file, and remove unused one. Related tracking issue is **TBD**.

# Alternatives

In the "Data Processing" section, we can enlarge the "no ingestion" period to include almost all the steps. This can simplify the entire procedure by a lot, but will bring a longer time of ingestion pause which may not be acceptable.
