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

## Testing

```bash
cargo nextest run -p query
cargo sqlness bare -t <case>
```

Check both standalone and distributed plans when changing `src/query/src/dist_plan/`.
