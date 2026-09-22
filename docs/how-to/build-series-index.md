# Build series indexes manually

`ADMIN BUILD_SERIES_INDEX('physical_table')` reconciles the local series indexes
on the current leaders of every data region in a physical Metric Engine table.
It returns `0` after all requested reconciliation passes succeed, or an error
if any region fails. Qualified table names are supported.

Enable `experimental_enable_series_index` in each target datanode's Mito
configuration first. The table must use sparse metric primary keys. Logical
metric tables and ordinary Mito tables are not accepted.

```sql
ADMIN FLUSH_TABLE('physical_table'); -- Optional: include buffered writes.
ADMIN BUILD_SERIES_INDEX('physical_table');
```

Each region captures its SST snapshot when its queued reconciliation starts.
Success means all eligible builds for that snapshot are complete, the catalogs
are persisted, and the index snapshot is published for readers. There is no
implicit flush, cluster-wide snapshot, or wait for concurrent writes or follower
replicas. Existing TTL and bucket eligibility rules still apply, including
requiring at least four SST files per bucket, skipping unknown sequence coverage,
and deferring series builds without a compaction window. Reusable indexes are
retained. Companion range indexes are also reconciled when
`experimental_enable_range_index` is enabled.

The operation shares a serial maintenance task with background reconciliation.
Client timeouts do not roll back completed work; retrying is safe. Background
maintenance continues on followers independently.

Upgrade all target datanodes before using this command. Older datanodes ignore
the new build options and can execute ordinary SST-index maintenance instead.
`ADMIN BUILD_INDEX('table')` retains its existing SST-index behavior.
