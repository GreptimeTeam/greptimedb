# query — Agent & Contributor Guide

Navigation map for `src/query`. Repo-wide invariants:
[`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

`query` owns SQL/PromQL/log planning, GreptimeDB optimizer rules, DataFusion
execution, and distributed query plans. Protocols live in `servers`; statement
side effects live in `operator`.

## Module map

| Area | Path | Entry point |
| --- | --- | --- |
| Logical planning | `src/query/src/planner.rs`, `src/query/src/parser.rs` | `LogicalPlanner`, `QueryStatement` |
| Query engine | `src/query/src/query_engine.rs`, `src/query/src/query_engine/` | Engine traits, factory, state, serialization |
| DataFusion execution | `src/query/src/datafusion.rs`, `src/query/src/datafusion/` | Physical planning and execution |
| Optimizers | `src/query/src/optimizer.rs`, `src/query/src/optimizer/` | GreptimeDB logical/physical rules |
| Distributed plans | `src/query/src/dist_plan.rs`, `src/query/src/dist_plan/` | `MergeScan`, pruning, merge/sort, remote filters |
| Remote reads | `src/query/src/region_query.rs` | `RegionQueryHandler` boundary |
| Query languages | `src/query/src/promql/`, `src/query/src/log_query/` | PromQL and log planners |

## Change coupling

- Optimizer or `MergeScan` changes usually touch rule ordering, physical
  planning, serialization, and distributed-plan tests together.
- User-visible SQL/PromQL/log behavior needs planner coverage and a sqlness case.
- `RegionQueryHandler` changes require the frontend implementation to move with
  it.
- DataFusion dependency changes follow the pinned-fork rule in the repo-wide
  invariants.
- Experimental native PromQL placement is process-local: remote admission covers
  single-source ordinary Mito base tables, with the frontend gathering ordinarily
  selected regions into one partition and merging raw samples by complete raw
  identity and time before native execution. Keep remote schema/label-decode
  validation strict; mixed-version datanodes that ignore control are rejected on
  schema mismatch. Datanode prefix pushdown, frontend repartition/hash shuffle,
  remote `NativeTree` serialization, metric-engine remote admission, and evaluated
  cross-region canonical merging beyond local `Merge` are not implemented.
  A key-and-time fan-in merge opens the selected region reads in parallel,
  consumes every opened stream continuously, and drops each stream at its own
  EOF so its datanode read permit is released progressively; it merges the live
  inputs by complete raw identity and time instead of materializing them first,
  and the chunks it does buffer are charged to the query memory pool, where
  exhaustion is an explicit resource error. The datanode read limit counts
  in-flight region read streams per datanode rather than per query, so a query
  selecting more regions on one datanode than its permit count is refused at
  admission and fails as an explicit error with every permit released and never
  a partial result; giving one query's whole read set a shared admission scope is
  admission-layer follow-up, not a frontend change.

## Testing

```bash
cargo nextest run -p query
cargo sqlness bare -t <case>
```

Check both standalone and distributed plans when changing `src/query/src/dist_plan/`.
