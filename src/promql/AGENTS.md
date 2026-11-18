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
supply multiple scan partitions. Where a boundary must answer one logical
series once, `SeriesRepartitionExec` merges that identity's fragments across
scan partitions and recomputes a single close for it. Cross-region
`SeriesMerge`/`Exchange` are still not implemented, so a logical series split
across regions is not merged. Do not describe partition-local work as a global
series merge.

Vector operations use the existing vector-matching semantics; do not describe
them as pairing only corresponding child series. A single-input operation can
independently map N series to N outputs. Aggregation placement must respect its
stage: `Complete` is valid only when the execution sees all contributions to
each group. Partitioned aggregation requires `Partial` state per input partition
followed by `Final` aggregation of that state. Do not claim that a complete
aggregate on each scan partition is globally complete.

The native `timestamp` operation evaluates on its input grid. A tree without
a source (a literal, `time()`, `vector(s)`) is materialized on its bound grid
without a scan. Native `count` produces Float64 values, unlike the legacy
PromQL path's Int64 count. Native SQL/TQL output is not necessarily
schema-identical to legacy output.

The native tree and its compiler describe the implemented model, not the whole
PromQL surface. Keep comments aligned with executable nodes and kernels rather
than listing an exhaustive function allowlist. In range functions,
`double_exponential_smoothing`, `mad_over_time`, and `ts_of_*_over_time` are
experimental in Prometheus; `first_over_time` is not.

## Change coupling

Distributed extension operators need matching handling in the Substrait
serializer (`src/common/substrait/src/extension_serializer.rs`), the physical
`PromExtensionPlanner`, and `check_extension_plan`
(`src/query/src/dist_plan/commutativity.rs`). Process-local native nodes must
not be treated as serializable or distributed operators. New logical extension
nodes also need a physical planner reachable from
`DfQueryPlanner::create_physical_plan`, assembled in
`src/query/src/query_engine/state.rs`.

A `FusedSeries` over more than one table stays on the frontend over one merge
scan per selector, each carrying a `RawSeriesSource` that answers explicit-label raw frames.
The frame schema is compared exactly at the merge scan, so its declaration and
the region's output change together.

## Gotchas

A series closes exactly once per input series, on the chunk that says so—not
once per final label set. A label-rewriting operation may cause distinct input
series to share final labels, and each still closes on its own chunk. The
closing chunk is the only way a consumer distinguishes “nothing more here”
from “no more input”; dropping it leaves a series open. Consumers must
preserve this per-series finality, including sparse trailing empty chunks. A
merge above them must not mistake a second close for a defect. A child that
merges regions must recompute finality globally rather than pass through a
region-local flag.

## Validation

```bash
cargo nextest run -p promql
cargo nextest run -p query          # planner and native-compile changes
cargo sqlness bare -t <case>        # PromQL cases under tests/cases/standalone/common/promql/
```
