# promql — Agent & Contributor Guide

Navigation map for `src/promql`. Repo-wide invariants:
[`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

`promql` owns PromQL evaluation: series/evaluated data, range and instant
functions, vector matching and extension operators. Statement planning lives in
`src/query/src/promql/`.

## Module map

| Area | Path | Entry point |
| --- | --- | --- |
| Data model | `src/promql/src/grid_series.rs`, `src/promql/src/evaluated.rs` | Grid series and evaluated chunks |
| Evaluation | `src/promql/src/range_eval.rs`, `src/promql/src/instant_eval.rs` | Range and instant evaluation |
| Kernels | `src/promql/src/functions.rs`, `src/promql/src/functions/` | PromQL functions |
| Matching | `src/promql/src/vector_match.rs` | Binary vector matching |
| Extension operators | `src/promql/src/extension_plan.rs`, `src/promql/src/extension_plan/` | Logical and physical extension nodes |
| Native model | `src/promql/src/native/` | Bound tree, execution, metrics and display |
| Native compiler | `src/query/src/promql/native_compile.rs` | PromQL AST to native tree |

## Native execution boundary

Native PromQL is an experimental, standalone evaluation route alongside the
existing DataFusion PromQL path. It operates on one storage region, which may
supply multiple scan partitions. It can evaluate each independent input series
in those partitions, but does not merge one logical series split across regions
or partitions; cross-region `SeriesMerge`/`Exchange` and global final-label
semantics are not implemented. Do not describe partition-local work as a global
series merge.

Pairwise vector operations consume corresponding child series (N inputs to
one output per pair); they do not globally join arbitrary series. A single-input
operation can independently map N series to N outputs. Aggregation placement
must respect its stage: `Complete` is valid only when the execution sees all
contributions to each group. Partitioned aggregation requires `Partial` state
per input partition followed by `Final` aggregation of that state. Do not claim
that a complete aggregate on each scan partition is globally complete.

The native `timestamp` operation evaluates on its input grid, and literal
scalar constants (including foldable constant expressions) bind on the
statement grid. This does not imply support for time-varying `time()` or
`scalar(v)`. Native `count` produces Float64 values, unlike the legacy PromQL
path's Int64 count. Native SQL/TQL output is not necessarily schema-identical
to legacy output.

The native tree and its compiler describe the implemented model, not the whole
PromQL surface. Keep comments aligned with executable nodes and kernels rather
than listing an exhaustive function allowlist. In range functions,
`double_exponential_smoothing`, `mad_over_time`, and `ts_of_*_over_time` are
experimental in Prometheus; `first_over_time` is not.

## Change coupling

Distributed extension operators need corresponding Substrait serialization,
physical planning, and distributed-plan validation. Process-local native nodes
must not be treated as serializable or distributed operators. New logical
extension nodes also need a physical planner reachable from
`DfQueryPlanner::create_physical_plan` in `src/query/src/query_engine/state.rs`.

## Validation

```bash
cargo nextest run -p promql
cargo nextest run -p query
```
